package com.relay.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskAttempt;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.TaskAttemptRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class WorkflowOrchestrator {

    private final WorkflowRepository workflowRepository;
    private final TaskRepository taskRepository;
    private final TaskAttemptRepository taskAttemptRepository;
    private final DependencyGraphResolver dependencyGraphResolver;
    private final TaskExecutionRegistry taskExecutionRegistry;
    private final RetryPolicy retryPolicy;
    private final ObjectMapper objectMapper;
    private final WorkflowAuditTracker workflowAuditTracker;

    public WorkflowOrchestrator(
        WorkflowRepository workflowRepository,
        TaskRepository taskRepository,
        TaskAttemptRepository taskAttemptRepository,
        DependencyGraphResolver dependencyGraphResolver,
        TaskExecutionRegistry taskExecutionRegistry,
        RetryPolicy retryPolicy,
        ObjectMapper objectMapper,
        WorkflowAuditTracker workflowAuditTracker
    ) {
        this.workflowRepository = workflowRepository;
        this.taskRepository = taskRepository;
        this.taskAttemptRepository = taskAttemptRepository;
        this.dependencyGraphResolver = dependencyGraphResolver;
        this.taskExecutionRegistry = taskExecutionRegistry;
        this.retryPolicy = retryPolicy;
        this.objectMapper = objectMapper;
        this.workflowAuditTracker = workflowAuditTracker;
    }

    @Transactional
    public Workflow createAndExecuteWorkflow(List<TaskDefinition> definitions) {
        if (definitions == null || definitions.isEmpty()) {
            throw new IllegalArgumentException("Workflow must contain at least one task");
        }

        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow.setVersion(1);
        workflowRepository.save(workflow);
        workflowAuditTracker.record(workflow, null, "workflow.created", "Workflow created", Map.of("taskCount", definitions.size(), "version", workflow.getVersion()));

        Map<String, Task> taskMap = new HashMap<>();
        for (int i = 0; i < definitions.size(); i++) {
            TaskDefinition definition = definitions.get(i);
            String referenceId = resolveReferenceId(definition, i);
            Task task = new Task();
            task.setWorkflow(workflow);
            workflow.addTask(task);
            task.setType(definition.getType());
            task.setAdapterType(definition.getAdapterType());
            task.setOwner(definition.getOwner());
            task.setEnvironment(definition.getEnvironment());
            task.setVersion(definition.getVersion());
            task.setPayload(toJson(definition.getPayload()));
            task.setStatus(TaskStatus.PENDING);
            task.setDependsOn(new UUID[0]);
            task.setAttemptCount(0);
            String idempotencyKey = definition.getIdempotencyKey() == null || definition.getIdempotencyKey().isBlank()
                ? UUID.randomUUID().toString()
                : definition.getIdempotencyKey();
            task.setIdempotencyKey(idempotencyKey);
            taskRepository.save(task);
            workflowAuditTracker.record(workflow, task.getId(), "task.created", "Task created", Map.of("taskType", task.getType(), "adapterType", task.getAdapterType(), "version", task.getVersion(), "idempotencyKey", idempotencyKey));
            taskMap.put(referenceId, task);
        }

        for (int i = 0; i < definitions.size(); i++) {
            TaskDefinition definition = definitions.get(i);
            String referenceId = resolveReferenceId(definition, i);
            Task task = taskMap.get(referenceId);
            List<UUID> dependencies = new ArrayList<>();

            if (definition.getDependsOn() != null) {
                for (String dependencyReference : definition.getDependsOn()) {
                    Task dependencyTask = taskMap.get(dependencyReference);
                    if (dependencyTask == null) {
                        throw new IllegalArgumentException(
                            "Task " + referenceId + " references unknown dependency " + dependencyReference
                        );
                    }
                    dependencies.add(dependencyTask.getId());
                }
            }

            task.setDependsOn(dependencies.toArray(new UUID[0]));
            taskRepository.save(task);
        }

        workflow = workflowRepository.findById(workflow.getId()).orElseThrow();
        return executeWorkflow(workflow.getId());
    }

    @Transactional
    public Workflow executeWorkflow(UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new IllegalArgumentException("Workflow not found: " + workflowId));

        if (workflow.getStatus() == WorkflowStatus.CANCELLED) {
            return workflow;
        }
        if (workflow.getStatus() == WorkflowStatus.PAUSED) {
            return workflow;
        }
        if (workflow.getStatus() == WorkflowStatus.FAILED) {
            return workflow;
        }

        if (hasExceededTimeout(workflow)) {
            workflow.setStatus(WorkflowStatus.FAILED);
            workflowRepository.save(workflow);
            workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow timed out", Map.of("status", workflow.getStatus().name(), "timeoutSeconds", workflow.getTimeoutSeconds()));
            return workflowRepository.findById(workflowId).orElseThrow();
        }

        workflow.setStatus(WorkflowStatus.RUNNING);
        workflowRepository.save(workflow);
        workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow started execution", Map.of("status", workflow.getStatus().name()));

        while (true) {
            workflow = workflowRepository.findById(workflowId).orElseThrow();
            if (workflow.getStatus() == WorkflowStatus.CANCELLED || workflow.getStatus() == WorkflowStatus.PAUSED) {
                return workflow;
            }
            if (hasExceededTimeout(workflow)) {
                workflow.setStatus(WorkflowStatus.FAILED);
                workflowRepository.save(workflow);
                workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow timed out", Map.of("status", workflow.getStatus().name(), "timeoutSeconds", workflow.getTimeoutSeconds()));
                return workflowRepository.findById(workflowId).orElseThrow();
            }

            List<Task> readyTasks = dependencyGraphResolver.getReadyTasks(workflow);

            if (readyTasks.isEmpty()) {
                workflow.setStatus(allTasksSucceeded(workflow) ? WorkflowStatus.COMPLETED : WorkflowStatus.FAILED);
                workflowRepository.save(workflow);
                workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow completed execution", Map.of("status", workflow.getStatus().name(), "tasks", workflow.getTasks().size()));
                return workflowRepository.findById(workflowId).orElseThrow();
            }

            for (Task task : readyTasks) {
                task.setStatus(TaskStatus.RUNNING);
                taskRepository.save(task);

                TaskAttempt attempt = new TaskAttempt();
                attempt.setTask(task);
                attempt.setStartedAt(Instant.now());
                attempt.setResult(null);
                attempt.setError(null);

                boolean shouldRetry = false;
                try {
                    TaskExecutor taskExecutor = taskExecutionRegistry.resolve(task);
                    TaskResult result = taskExecutor.execute(task);
                    attempt.setResult(result);
                    attempt.setFinishedAt(Instant.now());

                    if (result == TaskResult.SUCCESS) {
                        task.setStatus(TaskStatus.SUCCEEDED);
                    } else {
                        task.setStatus(TaskStatus.FAILED);
                        shouldRetry = retryPolicy.shouldRetry(task, task.getAttemptCount() + 1);
                    }
                } catch (Exception ex) {
                    task.setStatus(TaskStatus.FAILED);
                    attempt.setResult(TaskResult.FAILURE);
                    attempt.setError(ex.getMessage());
                    attempt.setFinishedAt(Instant.now());
                    shouldRetry = retryPolicy.shouldRetry(task, task.getAttemptCount() + 1);
                }

                int nextAttemptNumber = (task.getAttemptCount() == null ? 0 : task.getAttemptCount()) + 1;
                task.setAttemptCount(nextAttemptNumber);

                if (task.getStatus() == TaskStatus.FAILED) {
                    if (shouldRetry) {
                        task.setStatus(TaskStatus.PENDING);
                    } else {
                        task.setStatus(TaskStatus.DEAD_LETTERED);
                    }
                }

                taskRepository.save(task);
                taskAttemptRepository.save(attempt);
                workflowAuditTracker.record(workflow, task.getId(), "task.state.changed", "Task state updated", Map.of(
                    "taskType", task.getType(),
                    "status", task.getStatus().name(),
                    "attemptCount", task.getAttemptCount(),
                    "version", task.getVersion()
                ));

                if (task.getStatus() == TaskStatus.DEAD_LETTERED) {
                    workflow.setStatus(WorkflowStatus.FAILED);
                    workflowRepository.save(workflow);
                    workflowAuditTracker.record(workflow, task.getId(), "workflow.state.changed", "Workflow failed due to dead-lettered task", Map.of("status", workflow.getStatus().name(), "taskType", task.getType()));
                    return workflowRepository.findById(workflowId).orElseThrow();
                }
            }
        }
    }

    @Transactional
    public Workflow pauseWorkflow(UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new IllegalArgumentException("Workflow not found: " + workflowId));
        if (workflow.getStatus() == WorkflowStatus.COMPLETED || workflow.getStatus() == WorkflowStatus.FAILED || workflow.getStatus() == WorkflowStatus.CANCELLED) {
            throw new IllegalStateException("Workflow cannot be paused in status " + workflow.getStatus());
        }
        workflow.setStatus(WorkflowStatus.PAUSED);
        workflowRepository.save(workflow);
        workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow paused", Map.of("status", workflow.getStatus().name()));
        return workflowRepository.findById(workflowId).orElseThrow();
    }

    @Transactional
    public Workflow resumeWorkflow(UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new IllegalArgumentException("Workflow not found: " + workflowId));
        if (workflow.getStatus() != WorkflowStatus.PAUSED) {
            throw new IllegalStateException("Workflow must be paused before it can be resumed");
        }
        workflow.setStatus(WorkflowStatus.PENDING);
        workflowRepository.save(workflow);
        workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow resumed", Map.of("status", workflow.getStatus().name()));
        return workflowRepository.findById(workflowId).orElseThrow();
    }

    @Transactional
    public Workflow cancelWorkflow(UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new IllegalArgumentException("Workflow not found: " + workflowId));
        if (workflow.getStatus() == WorkflowStatus.COMPLETED || workflow.getStatus() == WorkflowStatus.FAILED || workflow.getStatus() == WorkflowStatus.CANCELLED) {
            return workflow;
        }
        workflow.setStatus(WorkflowStatus.CANCELLED);
        workflowRepository.save(workflow);
        workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow cancelled", Map.of("status", workflow.getStatus().name()));
        return workflowRepository.findById(workflowId).orElseThrow();
    }

    private boolean allTasksSucceeded(Workflow workflow) {
        for (Task task : workflow.getTasks()) {
            if (task.getStatus() != TaskStatus.SUCCEEDED) {
                return false;
            }
        }
        return !workflow.getTasks().isEmpty();
    }

    private boolean hasExceededTimeout(Workflow workflow) {
        if (workflow == null || workflow.getTimeoutSeconds() == null || workflow.getTimeoutSeconds() <= 0) {
            return false;
        }
        if (workflow.getCreatedAt() == null) {
            return false;
        }
        Instant timeoutInstant = workflow.getCreatedAt().plusSeconds(workflow.getTimeoutSeconds());
        return Instant.now().isAfter(timeoutInstant);
    }

    private String resolveReferenceId(TaskDefinition definition, int index) {
        if (definition.getId() != null && !definition.getId().trim().isEmpty()) {
            return definition.getId();
        }
        return "task-" + index;
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload == null ? Map.of() : payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize payload", e);
        }
    }
}
