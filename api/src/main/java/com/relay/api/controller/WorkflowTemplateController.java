package com.relay.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.api.dto.TaskDefinitionResponse;
import com.relay.api.dto.TaskResponse;
import com.relay.api.dto.WorkflowResponse;
import com.relay.api.dto.WorkflowSubmissionRequest;
import com.relay.api.dto.WorkflowTemplateResponse;
import com.relay.core.model.Task;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowTemplate;
import com.relay.core.model.WorkflowTemplateRequest;
import com.relay.core.service.WorkflowTemplateService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/workflow-templates")
public class WorkflowTemplateController {

    private final WorkflowTemplateService workflowTemplateService;
    private final ObjectMapper objectMapper;

    public WorkflowTemplateController(WorkflowTemplateService workflowTemplateService, ObjectMapper objectMapper) {
        this.workflowTemplateService = workflowTemplateService;
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public ResponseEntity<List<WorkflowTemplateResponse>> listTemplates() {
        return ResponseEntity.ok(workflowTemplateService.listTemplates().stream().map(this::toResponse).collect(Collectors.toList()));
    }

    @PostMapping
    public ResponseEntity<WorkflowTemplateResponse> createTemplate(@RequestBody WorkflowTemplateRequest request) {
        WorkflowTemplate template = workflowTemplateService.createTemplate(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(template));
    }

    @GetMapping("/{templateId}")
    public ResponseEntity<WorkflowTemplateResponse> getTemplate(@PathVariable UUID templateId) {
        WorkflowTemplate template = workflowTemplateService.getTemplate(templateId);
        return ResponseEntity.ok(toResponse(template));
    }

    @PostMapping("/{templateId}/submit")
    public ResponseEntity<WorkflowResponse> submitTemplate(@PathVariable UUID templateId, @RequestBody(required = false) WorkflowSubmissionRequest request) {
        Workflow workflow = workflowTemplateService.submitTemplate(
            templateId,
            request == null ? null : request.getOwner(),
            request == null ? null : request.getEnvironment(),
            request == null ? null : request.getTimeoutSeconds(),
            request == null ? null : request.getSlaThresholdSeconds()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(toWorkflowResponse(workflow));
    }

    private WorkflowTemplateResponse toResponse(WorkflowTemplate template) {
        WorkflowTemplateResponse response = new WorkflowTemplateResponse();
        response.setId(template.getId());
        response.setName(template.getName());
        response.setDescription(template.getDescription());
        response.setCategory(template.getCategory());
        response.setOwner(template.getOwner());
        response.setEnvironment(template.getEnvironment());
        response.setTimeoutSeconds(template.getTimeoutSeconds());
        response.setSlaThresholdSeconds(template.getSlaThresholdSeconds());
        response.setVersion(template.getVersion());
        response.setCreatedAt(template.getCreatedAt());
        response.setUpdatedAt(template.getUpdatedAt());

        List<TaskDefinition> tasks = template.getTaskDefinitionsAsList(objectMapper);
        response.setTasks(tasks.stream().map(this::toTaskDefinitionResponse).collect(Collectors.toList()));
        return response;
    }

    private TaskDefinitionResponse toTaskDefinitionResponse(TaskDefinition definition) {
        TaskDefinitionResponse response = new TaskDefinitionResponse();
        response.setId(definition.getId());
        response.setType(definition.getType());
        response.setAdapterType(definition.getAdapterType());
        response.setOwner(definition.getOwner());
        response.setEnvironment(definition.getEnvironment());
        response.setVersion(definition.getVersion());
        response.setIdempotencyKey(definition.getIdempotencyKey());
        response.setPayload(definition.getPayload());
        response.setDependsOn(definition.getDependsOn());
        return response;
    }

    private WorkflowResponse toWorkflowResponse(Workflow workflow) {
        WorkflowResponse response = new WorkflowResponse();
        response.setId(workflow.getId());
        response.setStatus(workflow.getStatus());
        response.setOwner(workflow.getOwner());
        response.setEnvironment(workflow.getEnvironment());
        response.setTimeoutSeconds(workflow.getTimeoutSeconds());
        response.setSlaThresholdSeconds(workflow.getSlaThresholdSeconds());
        response.setVersion(workflow.getVersion());
        response.setCreatedAt(workflow.getCreatedAt());
        response.setUpdatedAt(workflow.getUpdatedAt());

        if (workflow.getCreatedAt() != null && workflow.getUpdatedAt() != null) {
            response.setDurationMs(java.time.Duration.between(workflow.getCreatedAt(), workflow.getUpdatedAt()).toMillis());
        }

        List<Task> tasks = workflow.getTasks();
        response.setTotalTasks(tasks.size());
        response.setSucceededTasks((int) tasks.stream().filter(task -> task.getStatus() == TaskStatus.SUCCEEDED).count());
        response.setFailedTasks((int) tasks.stream().filter(task -> task.getStatus() == TaskStatus.FAILED || task.getStatus() == TaskStatus.DEAD_LETTERED).count());

        response.setTasks(tasks.stream().map(task -> {
            TaskResponse taskResponse = new TaskResponse();
            taskResponse.setId(task.getId());
            taskResponse.setType(task.getType());
            taskResponse.setAdapterType(task.getAdapterType());
            taskResponse.setOwner(task.getOwner());
            taskResponse.setEnvironment(task.getEnvironment());
            taskResponse.setVersion(task.getVersion());
            taskResponse.setStatus(task.getStatus());
            taskResponse.setAttemptCount(task.getAttemptCount() == null ? 0 : task.getAttemptCount());
            taskResponse.setCreatedAt(task.getCreatedAt());
            taskResponse.setUpdatedAt(task.getUpdatedAt());
            taskResponse.setPayload(parsePayload(task.getPayload()));
            taskResponse.setDependsOn(task.getDependsOn() == null ? java.util.List.of() : java.util.Arrays.stream(task.getDependsOn()).map(UUID::toString).collect(Collectors.toList()));
            return taskResponse;
        }).collect(Collectors.toList()));

        return response;
    }

    private java.util.Map<String, Object> parsePayload(String payload) {
        if (payload == null || payload.isBlank() || payload.equals("{}")) {
            return java.util.Map.of();
        }
        try {
            return objectMapper.readValue(payload, new com.fasterxml.jackson.core.type.TypeReference<>() {});
        } catch (Exception e) {
            return java.util.Map.of();
        }
    }
}
