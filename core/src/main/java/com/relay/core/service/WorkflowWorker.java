package com.relay.core.service;

import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.WorkflowRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class WorkflowWorker {

    private final WorkflowRepository workflowRepository;
    private final WorkflowOrchestrator workflowOrchestrator;

    public WorkflowWorker(WorkflowRepository workflowRepository, WorkflowOrchestrator workflowOrchestrator) {
        this.workflowRepository = workflowRepository;
        this.workflowOrchestrator = workflowOrchestrator;
    }

    @Scheduled(fixedDelayString = "${relay.worker.poll-delay:5000}")
    public void processPendingWorkflows() {
        List<Workflow> workflows = workflowRepository.findAll();
        for (Workflow workflow : workflows) {
            if (workflow.getStatus() == WorkflowStatus.PENDING || workflow.getStatus() == WorkflowStatus.RUNNING) {
                workflowOrchestrator.executeWorkflow(workflow.getId());
            }
        }
    }
}
