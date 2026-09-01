package com.relay.api.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.api.dto.TaskRequest;
import com.relay.api.dto.TaskResponse;
import com.relay.api.dto.WorkflowResponse;
import com.relay.api.dto.WorkflowSubmissionRequest;
import com.relay.core.model.Task;
import com.relay.core.model.TaskAttempt;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.TaskAttemptRepository;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/workflows")
public class WorkflowController {

    private final WorkflowOrchestrator workflowOrchestrator;
    private final WorkflowRepository workflowRepository;
    private final TaskRepository taskRepository;
    private final TaskAttemptRepository taskAttemptRepository;
    private final ObjectMapper objectMapper;

    public WorkflowController(
        WorkflowOrchestrator workflowOrchestrator,
        WorkflowRepository workflowRepository,
        TaskRepository taskRepository,
        TaskAttemptRepository taskAttemptRepository,
        ObjectMapper objectMapper
    ) {
        this.workflowOrchestrator = workflowOrchestrator;
        this.workflowRepository = workflowRepository;
        this.taskRepository = taskRepository;
        this.taskAttemptRepository = taskAttemptRepository;
        this.objectMapper = objectMapper;
    }

    @GetMapping
    public ResponseEntity<List<WorkflowResponse>> listWorkflows(
        @RequestParam(required = false) WorkflowStatus status,
        @RequestParam(required = false) String taskType,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size
    ) {
        List<Workflow> workflows = status == null
            ? workflowRepository.findAllByOrderByCreatedAtDesc()
            : workflowRepository.findByStatus(status);

        int pageSize = Math.max(1, Math.min(size, 100));
        int fromIndex = Math.min(page * pageSize, workflows.size());
        int toIndex = Math.min(fromIndex + pageSize, workflows.size());
        List<Workflow> pageContent = workflows.subList(fromIndex, toIndex);

        List<WorkflowResponse> responses = new ArrayList<>();
        for (Workflow workflow : pageContent) {
            WorkflowResponse response = toResponse(workflow, taskType);
            if (taskType == null || !response.getTasks().isEmpty()) {
                responses.add(response);
            }
        }
        return ResponseEntity.ok(responses);
    }

    @PostMapping
    public ResponseEntity<WorkflowResponse> createWorkflow(@RequestBody WorkflowSubmissionRequest request) {
        if (request == null || request.getTasks() == null || request.getTasks().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Workflow must include at least one task");
        }

        List<TaskDefinition> definitions = new ArrayList<>();
        for (TaskRequest taskRequest : request.getTasks()) {
            if (taskRequest == null || taskRequest.getType() == null || taskRequest.getType().isBlank()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Each task must include a type");
            }
            if (taskRequest.getId() != null && taskRequest.getId().isBlank()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Task id cannot be blank");
            }

            TaskDefinition definition = new TaskDefinition();
            definition.setId(taskRequest.getId());
            definition.setType(taskRequest.getType().trim());
            definition.setIdempotencyKey(taskRequest.getIdempotencyKey());
            definition.setPayload(taskRequest.getPayload());
            definition.setDependsOn(taskRequest.getDependsOn());
            definitions.add(definition);
        }

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(definitions);
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(workflow, null));
    }

    @GetMapping("/{workflowId}")
    public ResponseEntity<WorkflowResponse> getWorkflow(@PathVariable UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Workflow not found: " + workflowId));
        return ResponseEntity.ok(toResponse(workflow, null));
    }

    private WorkflowResponse toResponse(Workflow workflow) {
        return toResponse(workflow, null);
    }

    private WorkflowResponse toResponse(Workflow workflow, String taskTypeFilter) {
        WorkflowResponse response = new WorkflowResponse();
        response.setId(workflow.getId());
        response.setStatus(workflow.getStatus());
        response.setCreatedAt(workflow.getCreatedAt());
        response.setUpdatedAt(workflow.getUpdatedAt());
        if (workflow.getCreatedAt() != null && workflow.getUpdatedAt() != null) {
            response.setDurationMs(Duration.between(workflow.getCreatedAt(), workflow.getUpdatedAt()).toMillis());
        }

        List<Task> tasks = taskRepository.findByWorkflow_IdOrderByCreatedAtAsc(workflow.getId());
        if (taskTypeFilter != null && !taskTypeFilter.isBlank()) {
            tasks = tasks.stream()
                .filter(task -> taskTypeFilter.equalsIgnoreCase(task.getType()))
                .toList();
        }

        int succeeded = 0;
        int failed = 0;
        String lastError = null;

        for (Task task : tasks) {
            TaskResponse taskResponse = new TaskResponse();
            taskResponse.setId(task.getId());
            taskResponse.setType(task.getType());
            taskResponse.setStatus(task.getStatus());
            taskResponse.setAttemptCount(task.getAttemptCount() == null ? 0 : task.getAttemptCount());
            taskResponse.setCreatedAt(task.getCreatedAt());
            taskResponse.setUpdatedAt(task.getUpdatedAt());
            if (task.getCreatedAt() != null && task.getUpdatedAt() != null) {
                taskResponse.setDurationMs(Duration.between(task.getCreatedAt(), task.getUpdatedAt()).toMillis());
            }
            taskResponse.setDependsOn(toStrings(task.getDependsOn()));
            taskResponse.setPayload(parsePayload(task.getPayload()));

            List<TaskAttempt> attempts = taskAttemptRepository.findByTask_IdOrderByCreatedAtDesc(task.getId());
            if (!attempts.isEmpty()) {
                TaskAttempt latestAttempt = attempts.get(0);
                taskResponse.setResult(latestAttempt.getResult());
                taskResponse.setErrorMessage(latestAttempt.getError());
                if (latestAttempt.getError() != null && !latestAttempt.getError().isBlank()) {
                    lastError = latestAttempt.getError();
                }
            }

            if (task.getStatus() == TaskStatus.SUCCEEDED) {
                succeeded++;
            }
            if (task.getStatus() == TaskStatus.FAILED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
                failed++;
            }

            response.getTasks().add(taskResponse);
        }

        response.setTotalTasks(tasks.size());
        response.setSucceededTasks(succeeded);
        response.setFailedTasks(failed);
        response.setLastError(lastError);
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
