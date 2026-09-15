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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class TaskDispatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatchConsumer.class);

    private final TaskRepository taskRepository;
    private final TaskAttemptRepository taskAttemptRepository;
    private final WorkflowRepository workflowRepository;
    private final TaskExecutionRegistry taskExecutionRegistry;
    private final RetryPolicy retryPolicy;
    private final WorkflowAuditTracker workflowAuditTracker;
    private final ObjectMapper objectMapper;
    private final TaskExecutionGuard taskExecutionGuard;
    private final DeadLetterTaskService deadLetterTaskService;
    private final KafkaDispatchFailureService dispatchFailureService;
    private final IdempotencyService idempotencyService;
    private final OutboxService outboxService;
    private final KafkaRuntimeMetrics metrics;
    private final String taskTopic;

    public TaskDispatchConsumer(
        TaskRepository taskRepository,
        TaskAttemptRepository taskAttemptRepository,
        WorkflowRepository workflowRepository,
        TaskExecutionRegistry taskExecutionRegistry,
        RetryPolicy retryPolicy,
        WorkflowAuditTracker workflowAuditTracker,
        ObjectMapper objectMapper,
        TaskExecutionGuard taskExecutionGuard,
        DeadLetterTaskService deadLetterTaskService,
        KafkaDispatchFailureService dispatchFailureService,
        @Autowired(required = false) IdempotencyService idempotencyService,
        @Autowired(required = false) OutboxService outboxService,
        @Autowired(required = false) KafkaRuntimeMetrics metrics,
        @Value("${relay.kafka.task-topic:relay.workflow.tasks}") String taskTopic
    ) {
        this.taskRepository = taskRepository;
        this.taskAttemptRepository = taskAttemptRepository;
        this.workflowRepository = workflowRepository;
        this.taskExecutionRegistry = taskExecutionRegistry;
        this.retryPolicy = retryPolicy;
        this.workflowAuditTracker = workflowAuditTracker;
        this.objectMapper = objectMapper;
        this.taskExecutionGuard = taskExecutionGuard;
        this.deadLetterTaskService = deadLetterTaskService;
        this.dispatchFailureService = dispatchFailureService;
        this.idempotencyService = idempotencyService;
        this.outboxService = outboxService;
        this.metrics = metrics;
        this.taskTopic = taskTopic;
    }

    @KafkaListener(
        topics = "${relay.kafka.task-topic:relay.workflow.tasks}",
        groupId = "${relay.kafka.task-consumer.group-id:relay-workflow-task-group}"
    )
    @Transactional
    public void consume(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            recordPoison(payload, "empty-payload");
            return;
        }

        Object rawTaskId = payload.get("taskId");
        if (rawTaskId == null) {
            recordPoison(payload, "missing-task-id");
            return;
        }

        UUID taskId;
        try {
            taskId = UUID.fromString(String.valueOf(rawTaskId));
        } catch (IllegalArgumentException ex) {
            recordPoison(payload, "invalid-task-id");
            return;
        }

        Task task = taskRepository.findByIdWithWorkflow(taskId).orElse(null);
        if (task == null) {
            recordPoison(payload, "missing-task");
            return;
        }

        Object rawIdempotencyKey = payload.get("idempotencyKey");
        if (rawIdempotencyKey != null
            && task.getIdempotencyKey() != null
            && !task.getIdempotencyKey().equals(String.valueOf(rawIdempotencyKey))) {
            recordPoison(payload, "idempotency-key-mismatch");
            return;
        }

        if (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.DEAD_LETTERED
            || (idempotencyService != null && idempotencyService.shouldSkipExecution(task))) {
            if (metrics != null) {
                metrics.taskDuplicated();
            }
            workflowAuditTracker.record(task.getWorkflow(), task.getId(), "task.dispatch.duplicate", "Duplicate Kafka delivery ignored", Map.of(
                "status", task.getStatus().name(),
                "source", "kafka"
            ));
            return;
        }

        TaskExecutionGuard.ClaimDecision decision = taskExecutionGuard.decide(taskId);
        if (decision == TaskExecutionGuard.ClaimDecision.ALREADY_COMPLETE) {
            // Adapters must not re-apply side effects when the claim says already complete.
            if (metrics != null) {
                metrics.taskDuplicated();
            }
            return;
        }
        if (decision == TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE) {
            log.info("Skipping task {} because another worker holds an active execution lease", taskId);
            return;
        }
        if (decision != TaskExecutionGuard.ClaimDecision.CLAIMED) {
            recordPoison(payload, "claim-failed");
            return;
        }

        Task claimedTask = taskRepository.findByIdWithWorkflow(taskId).orElse(null);
        if (claimedTask == null) {
            recordPoison(payload, "missing-task");
            return;
        }

        if (metrics != null) {
            metrics.taskConsumed();
        }
        try {
            if (metrics == null) {
                executeTask(claimedTask);
            } else {
                metrics.time(() -> {
                    executeTask(claimedTask);
                    return null;
                });
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Kafka task execution failed for " + taskId, ex);
        }
    }

    private void recordPoison(Map<String, Object> payload, String reason) {
        if (metrics != null) {
            metrics.taskInvalid();
        }
        dispatchFailureService.record(taskTopic, toJson(payload), reason);
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload == null ? Map.of() : payload);
        } catch (JsonProcessingException ex) {
            return String.valueOf(payload);
        }
    }

    private void executeTask(Task task) {
        Workflow workflow = task.getWorkflow();
        if (workflow == null) {
            return;
        }

        if (idempotencyService != null && idempotencyService.shouldSkipExecution(task)) {
            if (metrics != null) {
                metrics.taskDuplicated();
            }
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
                task.setExecutionCompletedAt(Instant.now());
                clearClaim(task);
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
            if (metrics != null) {
                metrics.taskFailed();
            }
            clearClaim(task);
            if (shouldRetry) {
                task.setStatus(TaskStatus.PENDING);
                Instant retryAfter = retryPolicy.getNextAttemptAt(task, task.getAttemptCount());
                task.setNextAttemptAt(retryAfter);
                scheduleKafkaRetry(task, retryAfter);
                if (metrics != null) {
                    metrics.taskRetried();
                }
            } else {
                task.setStatus(TaskStatus.DEAD_LETTERED);
                task.setNextAttemptAt(null);
                task.setExecutionCompletedAt(Instant.now());
                deadLetterTaskService.record(task, attempt.getError());
                if (metrics != null) {
                    metrics.taskDeadLettered();
                }
            }
        }

        taskRepository.save(task);
        taskAttemptRepository.save(attempt);
        if (idempotencyService != null
            && (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.DEAD_LETTERED)) {
            idempotencyService.recordCompletion(task, attempt.getResult());
        }
        workflowAuditTracker.record(workflow, task.getId(), "task.state.changed", "Distributed task state updated", Map.of(
            "taskType", task.getType(),
            "status", task.getStatus().name(),
            "attemptCount", task.getAttemptCount(),
            "version", task.getVersion(),
            "source", "kafka"
        ));

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

    private void clearClaim(Task task) {
        task.setExecutionClaimedAt(null);
        task.setLockedBy(null);
        task.setLeaseExpiresAt(null);
    }

    private void scheduleKafkaRetry(Task task, Instant retryAfter) {
        if (outboxService == null || task.getWorkflow() == null) {
            return;
        }
        TaskDispatchMessage message = TaskDispatchMessage.fromTask(task);
        message.setRetryAfter(retryAfter == null ? Instant.now() : retryAfter);
        message.setAttemptNumber(task.getAttemptCount());
        outboxService.enqueueTaskRetry(task.getWorkflow().getId(), message, retryAfter);
        workflowAuditTracker.record(task.getWorkflow(), task.getId(), "task.retry.scheduled", "Task scheduled onto Kafka retry topic", Map.of(
            "attemptCount", task.getAttemptCount(),
            "retryAfter", message.getRetryAfter() == null ? null : message.getRetryAfter().toString()
        ));
    }
}
