package com.relay.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskAttempt;
import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.TaskAttemptRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true")
public class TaskDispatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatchConsumer.class);

    private final TaskRepository taskRepository;
    private final TaskAttemptRepository taskAttemptRepository;
    private final WorkflowRepository workflowRepository;
    private final TaskExecutionRegistry taskExecutionRegistry;
    private final RetryPolicy retryPolicy;
    private final WorkflowAuditTracker workflowAuditTracker;
    private final ObjectMapper objectMapper;

    public TaskDispatchConsumer(
        TaskRepository taskRepository,
        TaskAttemptRepository taskAttemptRepository,
        WorkflowRepository workflowRepository,
        TaskExecutionRegistry taskExecutionRegistry,
        RetryPolicy retryPolicy,
        WorkflowAuditTracker workflowAuditTracker,
        ObjectMapper objectMapper
    ) {
        this.taskRepository = taskRepository;
        this.taskAttemptRepository = taskAttemptRepository;
        this.workflowRepository = workflowRepository;
        this.taskExecutionRegistry = taskExecutionRegistry;
        this.retryPolicy = retryPolicy;
        this.workflowAuditTracker = workflowAuditTracker;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "${relay.kafka.task-topic:relay.workflow.tasks}",
        groupId = "${relay.kafka.task-consumer.group-id:relay-workflow-task-group}"
    )
    public void consume(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return;
        }

        Object rawTaskId = payload.get("taskId");
        if (rawTaskId == null) {
            return;
        }

        UUID taskId;
        try {
            taskId = UUID.fromString(String.valueOf(rawTaskId));
        } catch (IllegalArgumentException ex) {
            log.warn("Ignoring invalid task dispatch payload: {}", payload, ex);
            return;
        }

        Task task = taskRepository.findById(taskId).orElse(null);
        if (task == null) {
            log.warn("Received task dispatch for missing task {}", taskId);
            return;
        }

        if (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
            return;
        }

        executeTask(task);
    }

    private void executeTask(Task task) {
        Workflow workflow = task.getWorkflow();
        if (workflow == null) {
            return;
        }

        task.setStatus(TaskStatus.RUNNING);
        taskRepository.save(task);

        TaskAttempt attempt = new TaskAttempt();
        attempt.setTask(task);
        attempt.setStartedAt(Instant.now());
        attempt.setResult(null);
        attempt.setError(null);

        boolean shouldRetry = false;
        try {
            TaskResult result = taskExecutionRegistry.resolve(task).execute(task);
            attempt.setResult(result);
            attempt.setFinishedAt(Instant.now());
            if (result == TaskResult.SUCCESS) {
                task.setStatus(TaskStatus.SUCCEEDED);
                task.setNextAttemptAt(null);
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
                task.setNextAttemptAt(retryPolicy.getNextAttemptAt(task, task.getAttemptCount()));
            } else {
                task.setStatus(TaskStatus.DEAD_LETTERED);
                task.setNextAttemptAt(null);
            }
        }

        taskRepository.save(task);
        taskAttemptRepository.save(attempt);
        workflowAuditTracker.record(workflow, task.getId(), "task.state.changed", "Distributed task state updated", Map.of(
            "taskType", task.getType(),
            "status", task.getStatus().name(),
            "attemptCount", task.getAttemptCount(),
            "version", task.getVersion(),
            "source", "kafka"
        ));

        if (workflow != null) {
            if (task.getStatus() == TaskStatus.DEAD_LETTERED) {
                workflow.setStatus(WorkflowStatus.FAILED);
                workflowRepository.save(workflow);
                workflowAuditTracker.record(workflow, task.getId(), "workflow.state.changed", "Workflow failed due to dead-lettered task", Map.of("status", workflow.getStatus().name(), "taskType", task.getType()));
                return;
            }

            if (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.PENDING) {
                workflowRepository.save(workflow);
                workflowAuditTracker.record(workflow, null, "workflow.state.changed", "Workflow resumed after Kafka-dispatched task update", Map.of("status", workflow.getStatus().name()));
            }
        }
    }
}
