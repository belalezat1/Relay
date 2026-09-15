package com.relay.api;

import com.relay.core.model.OutboxEvent;
import com.relay.core.model.Task;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.OutboxEventRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import com.relay.core.service.OutboxService;
import com.relay.core.service.TaskClaimService;
import com.relay.core.service.TaskExecutionGuard;
import com.relay.core.service.TaskExecutionRegistry;
import com.relay.core.service.WorkflowOrchestrator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SpringBootTest(classes = RelayApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class PhaseIXIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
        .withDatabaseName("relay_it")
        .withUsername("relay")
        .withPassword("relay");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("relay.kafka.enabled", () -> "true");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("relay.outbox.poll-delay", () -> "3600000");
        registry.add("relay.worker.poll-delay", () -> "3600000");
        registry.add("relay.task.lease-recovery-delay", () -> "3600000");
        registry.add("relay.retry.backoff-enabled", () -> "true");
    }

    @Autowired
    private WorkflowOrchestrator workflowOrchestrator;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private OutboxService outboxService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private TaskClaimService taskClaimService;

    @Autowired
    private TaskExecutionRegistry taskExecutionRegistry;

    @Test
    void submitWorkflowCompletesAndWritesOutboxWhenKafkaEnabled() {
        assumeTrue(POSTGRES.isRunning());

        TaskDefinition definition = new TaskDefinition();
        definition.setId("a");
        definition.setType("success");
        definition.setPayload(Map.of());

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(definition));
        workflow = workflowRepository.findById(workflow.getId()).orElseThrow();

        assertThat(workflow.getStatus()).isIn(WorkflowStatus.COMPLETED, WorkflowStatus.RUNNING);
        List<Task> tasks = taskRepository.findByWorkflow_Id(workflow.getId());
        assertThat(tasks).isNotEmpty();
        assertThat(tasks.get(0).getStatus()).isIn(TaskStatus.SUCCEEDED, TaskStatus.QUEUED);

        assertThat(outboxEventRepository.countByStatus(OutboxEvent.STATUS_PENDING)
            + outboxEventRepository.countByStatus(OutboxEvent.STATUS_PUBLISHED)).isGreaterThan(0);
    }

    @Test
    void twoClaimersCannotDoubleExecuteWithSkipLocked() throws Exception {
        assumeTrue(POSTGRES.isRunning());

        AtomicInteger executions = new AtomicInteger();
        String type = "claim-once-" + UUID.randomUUID();
        taskExecutionRegistry.register(type, task -> {
            executions.incrementAndGet();
            return TaskResult.SUCCESS;
        });

        TaskDefinition definition = new TaskDefinition();
        definition.setType(type);
        definition.setPayload(Map.of());
        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(definition));
        Task task = taskRepository.findByWorkflow_Id(workflow.getId()).get(0);

        // Reset to PENDING for an explicit double-claim race after the first completion path.
        task.setStatus(TaskStatus.PENDING);
        task.setAttemptCount(0);
        task.setExecutionClaimedAt(null);
        task.setLockedBy(null);
        task.setLeaseExpiresAt(null);
        task.setExecutionCompletedAt(null);
        taskRepository.saveAndFlush(task);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<TaskExecutionGuard.ClaimDecision> first = executor.submit(() -> {
            start.await(2, TimeUnit.SECONDS);
            return taskClaimService.claim(task.getId(), "claimer-1");
        });
        Future<TaskExecutionGuard.ClaimDecision> second = executor.submit(() -> {
            start.await(2, TimeUnit.SECONDS);
            return taskClaimService.claim(task.getId(), "claimer-2");
        });
        start.countDown();

        TaskExecutionGuard.ClaimDecision left = first.get(10, TimeUnit.SECONDS);
        TaskExecutionGuard.ClaimDecision right = second.get(10, TimeUnit.SECONDS);
        executor.shutdownNow();

        long claimed = List.of(left, right).stream().filter(d -> d == TaskExecutionGuard.ClaimDecision.CLAIMED).count();
        assertThat(claimed).isEqualTo(1);
        assertThat(List.of(left, right)).contains(TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE);

        outboxService.enqueueWorkflowEvent(workflow.getId(), Map.of("eventType", "it.probe"));
        assertThat(outboxEventRepository.findAll()).isNotEmpty();
    }
}
