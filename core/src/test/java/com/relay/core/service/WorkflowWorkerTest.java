package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.WorkflowRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowWorkerTest {

    @Test
    void skipsDuplicateExecutionWhenWorkflowAlreadyInFlight() throws Exception {
        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID());
        workflow.setStatus(WorkflowStatus.PENDING);

        WorkflowRepository workflowRepository = createRepository(workflow);
        RecordingWorkflowOrchestrator workflowOrchestrator = new RecordingWorkflowOrchestrator(workflow.getId());
        WorkflowDispatchQueue queue = new WorkflowDispatchQueue();
        WorkflowWorker worker = new WorkflowWorker(workflowRepository, workflowOrchestrator, queue, 1, true);

        CompletableFuture<Void> firstRun = CompletableFuture.runAsync(worker::processPendingWorkflows);
        CompletableFuture<Void> secondRun = CompletableFuture.runAsync(worker::processPendingWorkflows);
        CompletableFuture.allOf(firstRun, secondRun).join();

        long deadline = System.currentTimeMillis() + 2000;
        while (workflowOrchestrator.invocations.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        // Give a second concurrent poll a chance to race after the first claim.
        Thread.sleep(100);
        worker.processPendingWorkflows();
        Thread.sleep(250);

        assertThat(workflowOrchestrator.invocations.get()).isEqualTo(1);
    }

    @Test
    void skipsOrchestrationWhenRoleIsConsumeOnly() {
        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID());
        workflow.setStatus(WorkflowStatus.PENDING);

        WorkflowRepository workflowRepository = createRepository(workflow);
        RecordingWorkflowOrchestrator workflowOrchestrator = new RecordingWorkflowOrchestrator(workflow.getId());
        WorkflowWorker worker = new WorkflowWorker(workflowRepository, workflowOrchestrator, new WorkflowDispatchQueue(), 1, false);

        worker.processPendingWorkflows();

        assertThat(worker.isOrchestrationEnabled()).isFalse();
        assertThat(workflowOrchestrator.invocations.get()).isZero();
    }

    private WorkflowRepository createRepository(Workflow workflow) {
        return (WorkflowRepository) Proxy.newProxyInstance(
            WorkflowRepository.class.getClassLoader(),
            new Class<?>[] { WorkflowRepository.class },
            (proxy, method, args) -> {
                if ("findAllByOrderByCreatedAtDesc".equals(method.getName())
                    || "findByStatusInOrderByCreatedAtAsc".equals(method.getName())) {
                    return List.of(workflow);
                }
                if ("findAll".equals(method.getName())) {
                    return List.of(workflow);
                }
                if ("findByStatus".equals(method.getName())) {
                    return List.of(workflow);
                }
                if ("count".equals(method.getName())) {
                    return 1L;
                }
                if ("save".equals(method.getName()) || "saveAll".equals(method.getName())) {
                    return args[0];
                }
                return null;
            }
        );
    }

    private static final class RecordingWorkflowOrchestrator extends WorkflowOrchestrator {
        private final AtomicInteger invocations = new AtomicInteger();
        private final UUID workflowId;

        private RecordingWorkflowOrchestrator(UUID workflowId) {
            super(null, null, null, null, null, null, new ObjectMapper(), new WorkflowAuditTracker(null, new ObjectMapper()), new NoOpTaskDispatchPublisher(), null, null);
            this.workflowId = workflowId;
        }

        @Override
        public Workflow executeWorkflow(UUID workflowId) {
            if (this.workflowId.equals(workflowId)) {
                invocations.incrementAndGet();
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Workflow workflow = new Workflow();
            workflow.setId(workflowId);
            workflow.setStatus(WorkflowStatus.RUNNING);
            return workflow;
        }
    }
}
