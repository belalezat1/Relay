package com.relay.core.service;

import com.relay.core.model.DeadLetterTask;
import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.DeadLetterTaskRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class DeadLetterTaskService {

    private final DeadLetterTaskRepository repository;
    private final TaskRepository taskRepository;
    private final WorkflowRepository workflowRepository;
    private final TaskDispatchPublisher taskDispatchPublisher;
    private final WorkflowAuditTracker workflowAuditTracker;

    public DeadLetterTaskService(DeadLetterTaskRepository repository) {
        this(repository, null, null, null, null);
    }

    @Autowired
    public DeadLetterTaskService(
        DeadLetterTaskRepository repository,
        @Autowired(required = false) TaskRepository taskRepository,
        @Autowired(required = false) WorkflowRepository workflowRepository,
        @Autowired(required = false) TaskDispatchPublisher taskDispatchPublisher,
        @Autowired(required = false) WorkflowAuditTracker workflowAuditTracker
    ) {
        this.repository = repository;
        this.taskRepository = taskRepository;
        this.workflowRepository = workflowRepository;
        this.taskDispatchPublisher = taskDispatchPublisher == null ? new NoOpTaskDispatchPublisher() : taskDispatchPublisher;
        this.workflowAuditTracker = workflowAuditTracker;
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

    @Transactional
    public DeadLetterTask replay(UUID deadLetterId) {
        if (taskRepository == null || workflowRepository == null) {
            throw new IllegalStateException("Dead-letter replay requires task and workflow repositories");
        }

        DeadLetterTask deadLetter = repository.findByIdWithTask(deadLetterId)
            .orElseThrow(() -> new IllegalArgumentException("Dead letter not found: " + deadLetterId));
        if (deadLetter.getReplayedAt() != null) {
            throw new IllegalStateException("Dead letter already replayed: " + deadLetterId);
        }

        Task task = deadLetter.getTask();
        if (task == null) {
            throw new IllegalStateException("Dead letter " + deadLetterId + " has no task");
        }

        task.setStatus(TaskStatus.PENDING);
        task.setExecutionClaimedAt(null);
        task.setExecutionCompletedAt(null);
        task.setLockedBy(null);
        task.setLeaseExpiresAt(null);
        task.setNextAttemptAt(null);
        taskRepository.save(task);

        Workflow workflow = task.getWorkflow();
        if (workflow != null
            && (workflow.getStatus() == WorkflowStatus.FAILED || workflow.getStatus() == WorkflowStatus.COMPLETED)) {
            workflow.setStatus(WorkflowStatus.PENDING);
            workflowRepository.save(workflow);
        }

        deadLetter.setReplayedAt(Instant.now());
        repository.save(deadLetter);

        if (taskDispatchPublisher.isEnabled()) {
            Task queued = taskRepository.findByIdWithWorkflow(task.getId()).orElse(task);
            queued.setStatus(TaskStatus.QUEUED);
            taskRepository.save(queued);
            taskDispatchPublisher.publish(queued);
        }

        if (workflowAuditTracker != null && workflow != null) {
            workflowAuditTracker.record(workflow, task.getId(), "task.dead_letter.replayed", "Dead-lettered task requeued", Map.of(
                "deadLetterId", deadLetterId.toString(),
                "status", TaskStatus.PENDING.name(),
                "kafka", taskDispatchPublisher.isEnabled()
            ));
        }

        return deadLetter;
    }
}
