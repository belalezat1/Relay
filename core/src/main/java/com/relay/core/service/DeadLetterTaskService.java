package com.relay.core.service;

import com.relay.core.model.DeadLetterTask;
import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.DeadLetterTaskRepository;
import com.relay.core.repository.IdempotencyOutcomeRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class DeadLetterTaskService {

    private final DeadLetterTaskRepository repository;
    private final TaskRepository taskRepository;
    private final WorkflowRepository workflowRepository;
    private final WorkflowAuditTracker workflowAuditTracker;
    private final IdempotencyOutcomeRepository idempotencyOutcomeRepository;

    public DeadLetterTaskService(DeadLetterTaskRepository repository) {
        this(repository, null, null, null, null);
    }

    public DeadLetterTaskService(
        DeadLetterTaskRepository repository,
        TaskRepository taskRepository,
        WorkflowRepository workflowRepository,
        WorkflowAuditTracker workflowAuditTracker
    ) {
        this(repository, taskRepository, workflowRepository, workflowAuditTracker, null);
    }

    @Autowired
    public DeadLetterTaskService(
        DeadLetterTaskRepository repository,
        @Autowired(required = false) TaskRepository taskRepository,
        @Autowired(required = false) WorkflowRepository workflowRepository,
        @Autowired(required = false) WorkflowAuditTracker workflowAuditTracker,
        @Autowired(required = false) IdempotencyOutcomeRepository idempotencyOutcomeRepository
    ) {
        this.repository = repository;
        this.taskRepository = taskRepository;
        this.workflowRepository = workflowRepository;
        this.workflowAuditTracker = workflowAuditTracker;
        this.idempotencyOutcomeRepository = idempotencyOutcomeRepository;
    }

    @Transactional
    public DeadLetterTask record(Task task, String error) {
        if (task == null || task.getId() == null || task.getWorkflow() == null
            || repository.existsByTask_IdAndReplayedAtIsNull(task.getId())) {
            return null;
        }

        DeadLetterTask deadLetter = new DeadLetterTask();
        deadLetter.setTask(task);
        deadLetter.setWorkflowId(task.getWorkflow().getId());
        deadLetter.setAttemptCount(task.getAttemptCount());
        deadLetter.setError(error == null || error.isBlank() ? "Task failed without an error message" : error);
        return repository.save(deadLetter);
    }

    @Transactional(readOnly = true)
    public List<DeadLetterTask> list(UUID workflowId) {
        return workflowId == null
            ? repository.findAllByOrderByCreatedAtDesc()
            : repository.findByWorkflowIdOrderByCreatedAtDesc(workflowId);
    }

    /**
     * Replay a dead-lettered task in place. Resets the task and (if needed) the workflow
     * in Postgres, deletes the DLQ row, and lets the orchestrator rediscover the work.
     * Re-runs the task side effect when execution resumes.
     */
    @Transactional
    public Task replay(UUID deadLetterId) {
        if (taskRepository == null || workflowRepository == null) {
            throw new IllegalStateException("Dead-letter replay requires task and workflow repositories");
        }

        DeadLetterTask deadLetter = repository.findByIdWithTask(deadLetterId)
            .orElseThrow(() -> new IllegalArgumentException("Dead letter not found: " + deadLetterId));

        Task task = deadLetter.getTask();
        if (task == null) {
            throw new IllegalStateException("Dead letter " + deadLetterId + " has no task");
        }
        if (task.getStatus() != TaskStatus.DEAD_LETTERED) {
            throw new IllegalStateException("Task is not dead-lettered: " + task.getId());
        }

        UUID taskId = task.getId();
        UUID workflowId = deadLetter.getWorkflowId();

        task.setStatus(TaskStatus.PENDING);
        task.setAttemptCount(0);
        task.setExecutionClaimedAt(null);
        task.setExecutionCompletedAt(null);
        task.setLockedBy(null);
        task.setLeaseExpiresAt(null);
        task.setNextAttemptAt(null);
        taskRepository.save(task);

        if (idempotencyOutcomeRepository != null && task.getIdempotencyKey() != null && !task.getIdempotencyKey().isBlank()) {
            idempotencyOutcomeRepository.findByIdempotencyKey(task.getIdempotencyKey())
                .ifPresent(idempotencyOutcomeRepository::delete);
        }

        Workflow workflow = task.getWorkflow();
        if (workflow == null && workflowId != null) {
            workflow = workflowRepository.findById(workflowId).orElse(null);
        }
        if (workflow != null && workflow.getStatus() == WorkflowStatus.FAILED) {
            workflow.setStatus(WorkflowStatus.PENDING);
            workflowRepository.save(workflow);
            if (workflowAuditTracker != null) {
                workflowAuditTracker.record(workflow, taskId, "workflow.state.changed", "Workflow reopened after dead-letter replay", Map.of(
                    "status", WorkflowStatus.PENDING.name(),
                    "deadLetterId", deadLetterId.toString()
                ));
            }
        }

        repository.delete(deadLetter);

        if (workflowAuditTracker != null && workflow != null) {
            workflowAuditTracker.record(workflow, taskId, "task.replayed", "Dead-lettered task reset for rediscovery", Map.of(
                "deadLetterId", deadLetterId.toString(),
                "status", TaskStatus.PENDING.name(),
                "attemptCount", 0
            ));
        }

        return taskRepository.findById(taskId).orElse(task);
    }
}
