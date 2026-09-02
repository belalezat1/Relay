package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowTemplate;
import com.relay.core.model.WorkflowTemplateRequest;
import com.relay.core.repository.WorkflowRepository;
import com.relay.core.repository.WorkflowTemplateRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class WorkflowTemplateService {

    private final WorkflowTemplateRepository workflowTemplateRepository;
    private final WorkflowRepository workflowRepository;
    private final WorkflowOrchestrator workflowOrchestrator;
    private final ObjectMapper objectMapper;

    public WorkflowTemplateService(
        WorkflowTemplateRepository workflowTemplateRepository,
        WorkflowRepository workflowRepository,
        WorkflowOrchestrator workflowOrchestrator,
        ObjectMapper objectMapper
    ) {
        this.workflowTemplateRepository = workflowTemplateRepository;
        this.workflowRepository = workflowRepository;
        this.workflowOrchestrator = workflowOrchestrator;
        this.objectMapper = objectMapper;
    }

    public List<WorkflowTemplate> listTemplates() {
        return workflowTemplateRepository.findAllByOrderByCreatedAtDesc();
    }

    public WorkflowTemplate getTemplate(UUID templateId) {
        return workflowTemplateRepository.findById(templateId)
            .orElseThrow(() -> new IllegalArgumentException("Workflow template not found: " + templateId));
    }

    @Transactional
    public WorkflowTemplate createTemplate(WorkflowTemplateRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Workflow template request is required");
        }
        if (request.getName() == null || request.getName().isBlank()) {
            throw new IllegalArgumentException("Workflow template name is required");
        }
        if (request.getTasks() == null || request.getTasks().isEmpty()) {
            throw new IllegalArgumentException("Workflow template must include at least one task");
        }

        WorkflowTemplate template = new WorkflowTemplate();
        template.setName(request.getName().trim());
        template.setDescription(request.getDescription());
        template.setCategory(request.getCategory());
        template.setOwner(request.getOwner());
        template.setEnvironment(request.getEnvironment());
        template.setTimeoutSeconds(request.getTimeoutSeconds());
        template.setSlaThresholdSeconds(request.getSlaThresholdSeconds());
        template.setTaskDefinitions(request.getTasks(), objectMapper);

        return workflowTemplateRepository.save(template);
    }

    @Transactional
    public Workflow submitTemplate(UUID templateId, String owner, String environment, Integer timeoutSeconds, Integer slaThresholdSeconds) {
        WorkflowTemplate template = getTemplate(templateId);
        List<TaskDefinition> definitions = template.getTaskDefinitionsAsList(objectMapper);
        if (definitions.isEmpty()) {
            throw new IllegalArgumentException("Workflow template does not define any tasks");
        }

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(definitions);

        String effectiveOwner = owner == null || owner.isBlank() ? template.getOwner() : owner;
        String effectiveEnvironment = environment == null || environment.isBlank() ? template.getEnvironment() : environment;
        Integer effectiveTimeoutSeconds = timeoutSeconds == null || timeoutSeconds <= 0 ? template.getTimeoutSeconds() : timeoutSeconds;
        Integer effectiveSlaThresholdSeconds = slaThresholdSeconds == null || slaThresholdSeconds <= 0 ? template.getSlaThresholdSeconds() : slaThresholdSeconds;

        workflow.setOwner(effectiveOwner);
        workflow.setEnvironment(effectiveEnvironment);
        workflow.setTimeoutSeconds(effectiveTimeoutSeconds);
        workflow.setSlaThresholdSeconds(effectiveSlaThresholdSeconds);
        return workflowRepository.save(workflow);
    }
}
