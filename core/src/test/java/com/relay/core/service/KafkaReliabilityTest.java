package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.DeadLetterTask;
import com.relay.core.model.IdempotencyOutcome;
import com.relay.core.model.KafkaDispatchFailure;
import com.relay.core.model.OutboxEvent;
import com.relay.core.model.Task;
import com.relay.core.model.TaskAttempt;
import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowAuditEvent;
import com.relay.core.model.WorkflowEventProjection;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.DeadLetterTaskRepository;
import com.relay.core.repository.IdempotencyOutcomeRepository;
import com.relay.core.repository.KafkaDispatchFailureRepository;
import com.relay.core.repository.OutboxEventRepository;
import com.relay.core.repository.TaskAttemptRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowAuditEventRepository;
import com.relay.core.repository.WorkflowEventProjectionRepository;
import com.relay.core.repository.WorkflowRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({
    TaskExecutionGuard.class,
    TaskClaimService.class,
    TaskLeaseRecoveryService.class,
    TaskExecutionRegistry.class,
    RetryPolicy.class,
    WorkflowAuditTracker.class,
    NoOpWorkflowEventPublisher.class,
    NoOpTaskDispatchPublisher.class,
    DeadLetterTaskService.class,
    KafkaDispatchFailureService.class,
    IdempotencyService.class,
    OutboxService.class,
    ObjectMapper.class,
    DependencyGraphResolver.class
})
@org.springframework.test.context.ContextConfiguration(classes = KafkaReliabilityTest.TestConfiguration.class)
class KafkaReliabilityTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableJpaRepositories(basePackageClasses = {
        WorkflowRepository.class,
        TaskRepository.class,
        TaskAttemptRepository.class,
        WorkflowAuditEventRepository.class,
        DeadLetterTaskRepository.class,
        KafkaDispatchFailureRepository.class,
        IdempotencyOutcomeRepository.class,
        OutboxEventRepository.class,
        WorkflowEventProjectionRepository.class
    })
    @EntityScan(basePackageClasses = {
        Workflow.class,
        Task.class,
        TaskAttempt.class,
        WorkflowAuditEvent.class,
        DeadLetterTask.class,
        KafkaDispatchFailure.class,
        IdempotencyOutcome.class,
        OutboxEvent.class,
        WorkflowEventProjection.class
    })
    static class TestConfiguration {
    }

    @Autowired
    private TaskExecutionGuard taskExecutionGuard;

    @Autowired
    private TaskClaimService taskClaimService;

    @Autowired
    private TaskLeaseRecoveryService taskLeaseRecoveryService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskAttemptRepository taskAttemptRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private KafkaDispatchFailureRepository kafkaDispatchFailureRepository;

    @Autowired
    private DeadLetterTaskRepository deadLetterTaskRepository;

    @Autowired
    private TaskExecutionRegistry taskExecutionRegistry;

    @Autowired
    private RetryPolicy retryPolicy;

    @Autowired
    private WorkflowAuditTracker workflowAuditTracker;

    @Autowired
    private DeadLetterTaskService deadLetterTaskService;

    @Autowired
    private KafkaDispatchFailureService kafkaDispatchFailureService;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DependencyGraphResolver dependencyGraphResolver;

    @Test
    void duplicateDeliveryOfSucceededTaskIsANoOp() {
        Task task = persistTask("success");
        TaskDispatchConsumer consumer = consumer();

        Map<String, Object> payload = dispatchPayload(task);
        consumer.consume(payload);
        consumer.consume(payload);

        Task reloaded = taskRepository.findById(task.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(reloaded.getAttemptCount()).isEqualTo(1);
        assertThat(taskAttemptRepository.findByTask_Id(reloaded.getId())).hasSize(1);
        assertThat(reloaded.getExecutionCompletedAt()).isNotNull();
    }

    @Test
    void concurrentConsumersOnlyExecuteOnce() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        taskExecutionRegistry.register("once_only", task -> {
            executions.incrementAndGet();
            return TaskResult.SUCCESS;
        });

        Task task = persistTask("once_only");
        TaskDispatchConsumer consumer = consumer();
        Map<String, Object> payload = dispatchPayload(task);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> first = executor.submit(() -> {
            await(start);
            consumer.consume(payload);
        });
        Future<?> second = executor.submit(() -> {
            await(start);
            consumer.consume(payload);
        });
        start.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertThat(executions.get()).isEqualTo(1);
        assertThat(taskRepository.findById(task.getId()).orElseThrow().getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void parallelClaimServiceDoesNotDoubleExecute() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        Task task = persistTask("success");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<TaskExecutionGuard.ClaimDecision> first = executor.submit(() -> {
            await(start);
            TaskExecutionGuard.ClaimDecision decision = taskClaimService.claim(task.getId(), "worker-a");
            if (decision == TaskExecutionGuard.ClaimDecision.CLAIMED) {
                executions.incrementAndGet();
            }
            return decision;
        });
        Future<TaskExecutionGuard.ClaimDecision> second = executor.submit(() -> {
            await(start);
            TaskExecutionGuard.ClaimDecision decision = taskClaimService.claim(task.getId(), "worker-b");
            if (decision == TaskExecutionGuard.ClaimDecision.CLAIMED) {
                executions.incrementAndGet();
            }
            return decision;
        });
        start.countDown();

        TaskExecutionGuard.ClaimDecision left = first.get(5, TimeUnit.SECONDS);
        TaskExecutionGuard.ClaimDecision right = second.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertThat(executions.get()).isEqualTo(1);
        assertThat(List.of(left, right)).contains(TaskExecutionGuard.ClaimDecision.CLAIMED);
        assertThat(List.of(left, right)).contains(TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE);
        Task claimed = taskRepository.findById(task.getId()).orElseThrow();
        assertThat(claimed.getLockedBy()).isNotBlank();
        assertThat(claimed.getLeaseExpiresAt()).isNotNull();
    }

    @Test
    void expiredClaimCanBeReclaimedAfterWorkerCrash() {
        Task task = persistTask("success");
        assertThat(taskExecutionGuard.decide(task.getId())).isEqualTo(TaskExecutionGuard.ClaimDecision.CLAIMED);
        assertThat(taskExecutionGuard.decide(task.getId())).isEqualTo(TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE);

        Task claimed = taskRepository.findById(task.getId()).orElseThrow();
        claimed.setExecutionClaimedAt(Instant.now().minusSeconds(taskExecutionGuard.getClaimLease().plusSeconds(1).getSeconds()));
        claimed.setLeaseExpiresAt(Instant.now().minusSeconds(1));
        taskRepository.save(claimed);

        assertThat(taskLeaseRecoveryService.recoverExpiredWork()).isGreaterThanOrEqualTo(1);
        Task recovered = taskRepository.findById(task.getId()).orElseThrow();
        assertThat(recovered.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(recovered.getExecutionClaimedAt()).isNull();
        assertThat(recovered.getLockedBy()).isNull();
        assertThat(taskExecutionGuard.decide(task.getId())).isEqualTo(TaskExecutionGuard.ClaimDecision.CLAIMED);
    }

    @Test
    void dependentTaskUnlocksOnlyAfterDurableSuccess() {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow = workflowRepository.saveAndFlush(workflow);

        Task first = new Task();
        first.setType("success");
        first.setStatus(TaskStatus.PENDING);
        first.setAttemptCount(0);
        first.setDependsOn(new UUID[0]);
        workflow.addTask(first);
        first = taskRepository.saveAndFlush(first);

        Task second = new Task();
        second.setType("success");
        second.setStatus(TaskStatus.PENDING);
        second.setAttemptCount(0);
        second.setDependsOn(new UUID[] { first.getId() });
        workflow.addTask(second);
        second = taskRepository.saveAndFlush(second);

        workflow.setTasks(taskRepository.findByWorkflow_IdOrderByCreatedAtAsc(workflow.getId()));
        assertThat(dependencyGraphResolver.getReadyTasks(workflow)).extracting(Task::getId).containsExactly(first.getId());

        consumer().consume(dispatchPayload(first));

        Task succeeded = taskRepository.findById(first.getId()).orElseThrow();
        assertThat(succeeded.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);

        workflow = workflowRepository.findById(workflow.getId()).orElseThrow();
        workflow.setTasks(taskRepository.findByWorkflow_IdOrderByCreatedAtAsc(workflow.getId()));
        assertThat(dependencyGraphResolver.getReadyTasks(workflow)).extracting(Task::getId).containsExactly(second.getId());
    }

    @Test
    void malformedAndMissingTaskPayloadsAreRecorded() {
        TaskDispatchConsumer consumer = consumer();
        consumer.consume(Map.of());
        consumer.consume(Map.of("taskId", "not-a-uuid"));
        consumer.consume(Map.of("taskId", UUID.randomUUID().toString()));

        assertThat(kafkaDispatchFailureRepository.findAll())
            .extracting(KafkaDispatchFailure::getReason)
            .contains("empty-payload", "invalid-task-id", "missing-task");
    }

    @Test
    void activeLeasePreventsASecondConsumerFromStartingSideEffects() {
        AtomicInteger executions = new AtomicInteger();
        taskExecutionRegistry.register("counted", task -> {
            executions.incrementAndGet();
            return TaskResult.SUCCESS;
        });
        Task task = persistTask("counted");
        assertThat(taskExecutionGuard.decide(task.getId())).isEqualTo(TaskExecutionGuard.ClaimDecision.CLAIMED);

        consumer().consume(dispatchPayload(task));

        assertThat(executions.get()).isZero();
        assertThat(taskRepository.findById(task.getId()).orElseThrow().getStatus()).isEqualTo(TaskStatus.RUNNING);
    }

    @Test
    void deadLetterReplayRequeuesTask() {
        Task task = persistTask("fail");
        task.setStatus(TaskStatus.DEAD_LETTERED);
        task.setAttemptCount(3);
        taskRepository.saveAndFlush(task);
        DeadLetterTask deadLetter = deadLetterTaskService.record(task, "boom");
        assertThat(deadLetter).isNotNull();

        DeadLetterTask replayed = deadLetterTaskService.replay(deadLetter.getId());
        assertThat(replayed.getReplayedAt()).isNotNull();

        Task reloaded = taskRepository.findById(task.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(reloaded.getExecutionClaimedAt()).isNull();
        assertThat(reloaded.getLockedBy()).isNull();
    }

    @Test
    void outboxEnqueuePersistsPendingEvent() {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow = workflowRepository.saveAndFlush(workflow);

        outboxService.enqueueWorkflowEvent(workflow.getId(), Map.of("eventType", "workflow.created"));

        assertThat(outboxEventRepository.findAll()).hasSize(1);
        assertThat(outboxEventRepository.findAll().get(0).getStatus()).isEqualTo(OutboxEvent.STATUS_PENDING);
    }

    private TaskDispatchConsumer consumer() {
        return new TaskDispatchConsumer(
            taskRepository,
            taskAttemptRepository,
            workflowRepository,
            taskExecutionRegistry,
            retryPolicy,
            workflowAuditTracker,
            objectMapper,
            taskExecutionGuard,
            deadLetterTaskService,
            kafkaDispatchFailureService,
            idempotencyService,
            null,
            null,
            "relay.workflow.tasks"
        );
    }

    private Task persistTask(String type) {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow = workflowRepository.saveAndFlush(workflow);

        Task task = new Task();
        task.setType(type);
        task.setStatus(TaskStatus.PENDING);
        task.setAttemptCount(0);
        task.setDependsOn(new UUID[0]);
        task.setIdempotencyKey(UUID.randomUUID().toString());
        workflow.addTask(task);
        return taskRepository.saveAndFlush(task);
    }

    private Map<String, Object> dispatchPayload(Task task) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", task.getId().toString());
        payload.put("workflowId", task.getWorkflow().getId().toString());
        payload.put("idempotencyKey", task.getIdempotencyKey());
        return payload;
    }

    private void await(CountDownLatch latch) {
        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
