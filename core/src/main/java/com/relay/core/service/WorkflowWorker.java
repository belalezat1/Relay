package com.relay.core.service;

import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.WorkflowRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

@Component
public class WorkflowWorker {

    private static final List<WorkflowStatus> ACTIVE_STATUSES = List.of(
        WorkflowStatus.PENDING,
        WorkflowStatus.RUNNING
    );

    private final WorkflowRepository workflowRepository;
    private final WorkflowOrchestrator workflowOrchestrator;
    private final WorkflowDispatchQueue dispatchQueue;
    private final Semaphore concurrencyLimit;
    private final Set<UUID> inFlightWorkflows = ConcurrentHashMap.newKeySet();
    private final boolean orchestrationEnabled;
    private final int batchSize;

    public WorkflowWorker(
        WorkflowRepository workflowRepository,
        WorkflowOrchestrator workflowOrchestrator,
        WorkflowDispatchQueue dispatchQueue,
        int maxConcurrency,
        boolean orchestrationEnabled
    ) {
        this(workflowRepository, workflowOrchestrator, dispatchQueue, maxConcurrency, orchestrationEnabled, 200);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public WorkflowWorker(
        WorkflowRepository workflowRepository,
        WorkflowOrchestrator workflowOrchestrator,
        WorkflowDispatchQueue dispatchQueue,
        @Value("${relay.worker.max-concurrency:4}") int maxConcurrency,
        @Value("${relay.worker.orchestration-enabled:true}") boolean orchestrationEnabled,
        @Value("${relay.worker.batch-size:200}") int batchSize
    ) {
        this.workflowRepository = workflowRepository;
        this.workflowOrchestrator = workflowOrchestrator;
        this.dispatchQueue = dispatchQueue;
        this.concurrencyLimit = new Semaphore(Math.max(1, maxConcurrency));
        this.orchestrationEnabled = orchestrationEnabled;
        this.batchSize = Math.max(1, batchSize);
    }

    public boolean isOrchestrationEnabled() {
        return orchestrationEnabled;
    }

    @Scheduled(fixedDelayString = "${relay.worker.poll-delay:200}")
    public void processPendingWorkflows() {
        if (!orchestrationEnabled) {
            return;
        }

        List<Workflow> workflows = workflowRepository.findByStatusInOrderByCreatedAtAsc(
            ACTIVE_STATUSES,
            PageRequest.of(0, batchSize)
        );
        for (Workflow workflow : workflows) {
            if (workflow.getStatus() == null) {
                continue;
            }
            if ((workflow.getStatus() == WorkflowStatus.PENDING || workflow.getStatus() == WorkflowStatus.RUNNING)
                && !inFlightWorkflows.contains(workflow.getId())) {
                dispatchQueue.tryQueue(workflow.getId());
            }
        }

        UUID workflowId;
        while ((workflowId = dispatchQueue.pollNext()) != null) {
            if (!inFlightWorkflows.add(workflowId)) {
                continue;
            }
            if (!concurrencyLimit.tryAcquire()) {
                inFlightWorkflows.remove(workflowId);
                dispatchQueue.tryQueue(workflowId);
                break;
            }

            final UUID workflowToRun = workflowId;
            CompletableFuture.runAsync(() -> {
                try {
                    workflowOrchestrator.executeWorkflow(workflowToRun);
                } finally {
                    inFlightWorkflows.remove(workflowToRun);
                    concurrencyLimit.release();
                }
            });
        }
    }
}
