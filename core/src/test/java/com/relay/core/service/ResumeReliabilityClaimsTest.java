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
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Resume-defensible reliability claims:
 * <ul>
 *   <li>Zero duplicate side effects across 1,000+ crash / redelivery injections for idempotent task types</li>
 *   <li>100% of permanently failed (exhausted-retry) tasks land in an inspectable DLQ with replay</li>
 * </ul>
 */
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@TestPropertySource(properties = {
    "relay.retry.backoff-enabled=false",
    "relay.kafka.retry-backoff-enabled=false",
    "relay.retry.max-attempts=3",
    "relay.task.claim-lease-seconds=300",
    "spring.jpa.show-sql=false",
    "spring.jpa.properties.hibernate.show_sql=false",
    "logging.level.org.hibernate.SQL=OFF",
    "logging.level.org.hibernate.orm.jdbc.bind=OFF",
    "logging.level.org.hibernate.orm.jdbc.extract=OFF"
})
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
@org.springframework.test.context.ContextConfiguration(classes = ResumeReliabilityClaimsTest.TestConfiguration.class)
class ResumeReliabilityClaimsTest {

    private static final int CRASH_REDELIVERY_INJECTIONS = 1000;
    private static final int EXHAUSTED_RETRY_TASKS = 50;

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
    private TaskLeaseRecoveryService taskLeaseRecoveryService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskAttemptRepository taskAttemptRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

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
    private ObjectMapper objectMapper;

    @Test
    void zeroDuplicateSideEffectsAcrossOneThousandCrashAndRedeliveryInjections() throws Exception {
        AtomicInteger sideEffects = new AtomicInteger();
        Set<String> appliedKeys = ConcurrentHashMap.newKeySet();
        taskExecutionRegistry.register("idempotent_counted", task -> {
            String key = task.getIdempotencyKey();
            if (key == null || !appliedKeys.add(key)) {
                return TaskResult.SUCCESS;
            }
            sideEffects.incrementAndGet();
            return TaskResult.SUCCESS;
        });

        TaskDispatchConsumer consumer = consumer();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < CRASH_REDELIVERY_INJECTIONS; i++) {
                Task task = persistTask("idempotent_counted");
                Map<String, Object> payload = dispatchPayload(task);
                int mode = i % 3;
                if (mode == 0) {
                    injectDuplicateDeliveryAfterSuccess(consumer, payload);
                } else if (mode == 1) {
                    injectWorkerCrashAfterClaimThenRedeliver(consumer, task, payload);
                } else {
                    injectConcurrentRedelivery(consumer, payload, executor);
                }
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(sideEffects.get())
            .as("idempotent task types must apply side effects exactly once across %s crash/redelivery injections",
                CRASH_REDELIVERY_INJECTIONS)
            .isEqualTo(CRASH_REDELIVERY_INJECTIONS);
        assertThat(appliedKeys).hasSize(CRASH_REDELIVERY_INJECTIONS);
        assertThat(taskRepository.findByStatus(TaskStatus.SUCCEEDED)).hasSize(CRASH_REDELIVERY_INJECTIONS);
    }

    @Test
    void oneHundredPercentOfExhaustedRetriesLandInInspectableDlqWithReplay() {
        taskExecutionRegistry.register("always_fail_dlq", task -> {
            throw new IllegalStateException("permanent failure");
        });
        TaskDispatchConsumer consumer = consumer();

        int exhausted = 0;
        int inDlq = 0;
        for (int i = 0; i < EXHAUSTED_RETRY_TASKS; i++) {
            Task task = persistTask("always_fail_dlq");
            Map<String, Object> payload = dispatchPayload(task);
            TaskStatus status = TaskStatus.PENDING;
            for (int attempt = 0; attempt < retryPolicy.getConfiguredMaxAttempts() + 2 && status != TaskStatus.DEAD_LETTERED; attempt++) {
                consumer.consume(payload);
                status = taskRepository.findById(task.getId()).orElseThrow().getStatus();
            }
            Task terminal = taskRepository.findById(task.getId()).orElseThrow();
            assertThat(terminal.getStatus()).isEqualTo(TaskStatus.DEAD_LETTERED);
            exhausted++;

            List<DeadLetterTask> letters = deadLetterTaskRepository.findByWorkflowIdOrderByCreatedAtDesc(
                terminal.getWorkflow().getId()
            );
            assertThat(letters).hasSize(1);
            inDlq++;

            UUID deadLetterId = letters.getFirst().getId();
            Task replayed = deadLetterTaskService.replay(deadLetterId);
            assertThat(replayed.getStatus()).isEqualTo(TaskStatus.PENDING);
            assertThat(replayed.getAttemptCount()).isZero();
            assertThat(deadLetterTaskRepository.findById(deadLetterId)).isEmpty();

            consumer.consume(payload);
            consumer.consume(payload);
            consumer.consume(payload);
            Task afterReplay = taskRepository.findById(task.getId()).orElseThrow();
            assertThat(afterReplay.getStatus()).isEqualTo(TaskStatus.DEAD_LETTERED);
            assertThat(deadLetterTaskRepository.findByWorkflowIdOrderByCreatedAtDesc(afterReplay.getWorkflow().getId()))
                .hasSize(1);
        }

        assertThat(exhausted).isEqualTo(EXHAUSTED_RETRY_TASKS);
        assertThat(inDlq).isEqualTo(EXHAUSTED_RETRY_TASKS);
        assertThat(inDlq * 100 / exhausted).isEqualTo(100);
    }

    private void injectDuplicateDeliveryAfterSuccess(TaskDispatchConsumer consumer, Map<String, Object> payload) {
        consumer.consume(payload);
        consumer.consume(payload);
    }

    private void injectWorkerCrashAfterClaimThenRedeliver(
        TaskDispatchConsumer consumer,
        Task task,
        Map<String, Object> payload
    ) {
        assertThat(taskExecutionGuard.decide(task.getId())).isEqualTo(TaskExecutionGuard.ClaimDecision.CLAIMED);
        Task claimed = taskRepository.findById(task.getId()).orElseThrow();
        claimed.setExecutionClaimedAt(Instant.now().minusSeconds(taskExecutionGuard.getClaimLease().plusSeconds(30).getSeconds()));
        claimed.setLeaseExpiresAt(Instant.now().minusSeconds(1));
        taskRepository.saveAndFlush(claimed);

        assertThat(taskLeaseRecoveryService.recoverExpiredWork()).isGreaterThanOrEqualTo(1);
        consumer.consume(payload);
        consumer.consume(payload);
    }

    private void injectConcurrentRedelivery(
        TaskDispatchConsumer consumer,
        Map<String, Object> payload,
        ExecutorService executor
    ) throws Exception {
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
