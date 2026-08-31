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

    public WorkflowOrchestrator(
        WorkflowRepository workflowRepository,
        TaskRepository taskRepository,
        TaskAttemptRepository taskAttemptRepository,
        DependencyGraphResolver dependencyGraphResolver,
        TaskExecutionRegistry taskExecutionRegistry,
        RetryPolicy retryPolicy,
        ObjectMapper objectMapper
    ) {
        this.workflowRepository = workflowRepository;
        this.taskRepository = taskRepository;
        this.taskAttemptRepository = taskAttemptRepository;
        this.dependencyGraphResolver = dependencyGraphResolver;
        this.taskExecutionRegistry = taskExecutionRegistry;
        this.retryPolicy = retryPolicy;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Workflow createAndExecuteWorkflow(List<TaskDefinition> definitions) {
        if (definitions == null || definitions.isEmpty()) {
            throw new IllegalArgumentException("Workflow must contain at least one task");
        }

        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflowRepository.save(workflow);

        Map<String, Task> taskMap = new HashMap<>();
        for (int i = 0; i < definitions.size(); i++) {
            TaskDefinition definition = definitions.get(i);
            String referenceId = resolveReferenceId(definition, i);
            Task task = new Task();
            task.setWorkflow(workflow);
            workflow.addTask(task);
            task.setType(definition.getType());
            task.setPayload(toJson(definition.getPayload()));
            task.setStatus(TaskStatus.PENDING);
            task.setDependsOn(new UUID[0]);
            task.setAttemptCount(0);
            task.setIdempotencyKey(UUID.randomUUID().toString());
            taskRepository.save(task);
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

        workflow.setStatus(WorkflowStatus.RUNNING);
        workflowRepository.save(workflow);

        while (true) {
            workflow = workflowRepository.findById(workflowId).orElseThrow();
            List<Task> readyTasks = dependencyGraphResolver.getReadyTasks(workflow);

            if (readyTasks.isEmpty()) {
                workflow.setStatus(allTasksSucceeded(workflow) ? WorkflowStatus.COMPLETED : WorkflowStatus.FAILED);
                workflowRepository.save(workflow);
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
                    TaskExecutor taskExecutor = taskExecutionRegistry.resolve(task.getType());
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

                if (task.getStatus() == TaskStatus.DEAD_LETTERED) {
                    workflow.setStatus(WorkflowStatus.FAILED);
                    workflowRepository.save(workflow);
                    return workflowRepository.findById(workflowId).orElseThrow();
                }
            }
        }
    }

    private boolean allTasksSucceeded(Workflow workflow) {
        for (Task task : workflow.getTasks()) {
            if (task.getStatus() != TaskStatus.SUCCEEDED) {
                return false;
            }
        }
        return !workflow.getTasks().isEmpty();
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
