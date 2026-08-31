package com.relay.api.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.api.dto.TaskRequest;
import com.relay.api.dto.TaskResponse;
import com.relay.api.dto.WorkflowResponse;
import com.relay.api.dto.WorkflowSubmissionRequest;
import com.relay.core.model.Task;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.Workflow;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import com.relay.core.service.WorkflowOrchestrator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/workflows")
public class WorkflowController {

    private final WorkflowOrchestrator workflowOrchestrator;
    private final WorkflowRepository workflowRepository;
    private final TaskRepository taskRepository;
    private final ObjectMapper objectMapper;

    public WorkflowController(
        WorkflowOrchestrator workflowOrchestrator,
        WorkflowRepository workflowRepository,
        TaskRepository taskRepository,
        ObjectMapper objectMapper
    ) {
        this.workflowOrchestrator = workflowOrchestrator;
        this.workflowRepository = workflowRepository;
        this.taskRepository = taskRepository;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<WorkflowResponse> createWorkflow(@RequestBody WorkflowSubmissionRequest request) {
        List<TaskDefinition> definitions = new ArrayList<>();
        for (TaskRequest taskRequest : request.getTasks()) {
            TaskDefinition definition = new TaskDefinition();
            definition.setId(taskRequest.getId());
            definition.setType(taskRequest.getType());
            definition.setPayload(taskRequest.getPayload());
            definition.setDependsOn(taskRequest.getDependsOn());
            definitions.add(definition);
        }

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(definitions);
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(workflow));
    }

    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowResponse> getWorkflow(@PathVariable UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Workflow not found: " + workflowId));
        return ResponseEntity.ok(toResponse(workflow));
    }

    private WorkflowResponse toResponse(Workflow workflow) {
        WorkflowResponse response = new WorkflowResponse();
        response.setId(workflow.getId());
        response.setStatus(workflow.getStatus());
        response.setCreatedAt(workflow.getCreatedAt());
        response.setUpdatedAt(workflow.getUpdatedAt());

        List<Task> tasks = taskRepository.findByWorkflow_Id(workflow.getId());
        for (Task task : tasks) {
            TaskResponse taskResponse = new TaskResponse();
            taskResponse.setId(task.getId());
            taskResponse.setType(task.getType());
            taskResponse.setStatus(task.getStatus());
            taskResponse.setAttemptCount(task.getAttemptCount() == null ? 0 : task.getAttemptCount());
            taskResponse.setCreatedAt(task.getCreatedAt());
            taskResponse.setUpdatedAt(task.getUpdatedAt());
            taskResponse.setDependsOn(toStrings(task.getDependsOn()));
            taskResponse.setPayload(parsePayload(task.getPayload()));

            response.getTasks().add(taskResponse);
        }
        return response;
    }

    private Map<String, Object> parsePayload(String payload) {
        if (payload == null || payload.isBlank() || payload.equals("{}")) {
            return Map.of();
        }

        try {
            return objectMapper.readValue(payload, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            return Map.of();
        }
    }

    private List<String> toStrings(UUID[] values) {
        List<String> output = new ArrayList<>();
        if (values == null) {
            return output;
        }
        for (UUID value : values) {
            output.add(String.valueOf(value));
        }
        return output;
    }
}
