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
        WorkflowWorker worker = new WorkflowWorker(workflowRepository, workflowOrchestrator, queue, 1);

        CompletableFuture<Void> firstRun = CompletableFuture.runAsync(worker::processPendingWorkflows);
        CompletableFuture<Void> secondRun = CompletableFuture.runAsync(worker::processPendingWorkflows);
        CompletableFuture.allOf(firstRun, secondRun).join();

        assertThat(workflowOrchestrator.invocations.get()).isEqualTo(1);
    }

    private WorkflowRepository createRepository(Workflow workflow) {
        return (WorkflowRepository) Proxy.newProxyInstance(
            WorkflowRepository.class.getClassLoader(),
            new Class<?>[] { WorkflowRepository.class },
            (proxy, method, args) -> {
                if ("findAllByOrderByCreatedAtDesc".equals(method.getName())) {
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
            super(null, null, null, null, null, null, new ObjectMapper(), new WorkflowAuditTracker(null, new ObjectMapper()));
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
