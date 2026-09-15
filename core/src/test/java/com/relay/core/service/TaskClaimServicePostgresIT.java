package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@Import(TaskClaimService.class)
@org.springframework.test.context.ContextConfiguration(classes = TaskClaimServicePostgresIT.TestConfiguration.class)
class TaskClaimServicePostgresIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
        .withDatabaseName("relay_claim")
        .withUsername("relay")
        .withPassword("relay");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.flyway.enabled", () -> "false");
        registry.add("spring.jpa.properties.hibernate.dialect", () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("relay.task.claim.skip-locked", () -> "true");
        registry.add("relay.task.claim-lease-seconds", () -> "300");
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableJpaRepositories(basePackageClasses = {TaskRepository.class, WorkflowRepository.class})
    @EntityScan(basePackageClasses = {Task.class, Workflow.class})
    static class TestConfiguration {
    }

    @Autowired
    private TaskClaimService taskClaimService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Test
    void skipLockedAllowsOnlyOneClaimer() throws Exception {
        assumeTrue(POSTGRES.isRunning());

        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow = workflowRepository.saveAndFlush(workflow);

        Task task = new Task();
        task.setType("success");
        task.setStatus(TaskStatus.PENDING);
        task.setAttemptCount(0);
        task.setDependsOn(new UUID[0]);
        task.setIdempotencyKey(UUID.randomUUID().toString());
        workflow.addTask(task);
        task = taskRepository.saveAndFlush(task);

        AtomicInteger claimed = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        UUID taskId = task.getId();
        Future<?> first = executor.submit(() -> {
            start.await(2, TimeUnit.SECONDS);
            if (taskClaimService.claim(taskId, "w1") == TaskExecutionGuard.ClaimDecision.CLAIMED) {
                claimed.incrementAndGet();
            }
            return null;
        });
        Future<?> second = executor.submit(() -> {
            start.await(2, TimeUnit.SECONDS);
            if (taskClaimService.claim(taskId, "w2") == TaskExecutionGuard.ClaimDecision.CLAIMED) {
                claimed.incrementAndGet();
            }
            return null;
        });
        start.countDown();
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertThat(claimed.get()).isEqualTo(1);
        assertThat(taskRepository.findById(taskId).orElseThrow().getLockedBy()).isIn("w1", "w2");
    }
}
