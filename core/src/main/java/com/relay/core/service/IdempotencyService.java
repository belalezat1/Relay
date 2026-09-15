package com.relay.core.service;

import com.relay.core.model.IdempotencyOutcome;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;
import com.relay.core.repository.IdempotencyOutcomeRepository;
import com.relay.core.repository.TaskRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Enforces task idempotency:
 * <ul>
 *   <li>Skip side effects when a task is already SUCCEEDED/DEAD_LETTERED</li>
 *   <li>Persist completed outcomes keyed by {@code idempotency_key}</li>
 *   <li>Reject submit of a duplicate key while a non-terminal task still exists</li>
 * </ul>
 * Adapters should treat {@link TaskExecutionGuard.ClaimDecision#ALREADY_COMPLETE} as a
 * no-op and must not re-apply external side effects.
 */
@Service
public class IdempotencyService {

    private final IdempotencyOutcomeRepository outcomeRepository;
    private final TaskRepository taskRepository;

    public IdempotencyService(IdempotencyOutcomeRepository outcomeRepository, TaskRepository taskRepository) {
        this.outcomeRepository = outcomeRepository;
        this.taskRepository = taskRepository;
    }

    @Transactional(readOnly = true)
    public void assertNoActiveDuplicate(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return;
        }
        List<Task> active = taskRepository.findNonTerminalByIdempotencyKey(idempotencyKey.trim());
        if (!active.isEmpty()) {
            throw new IllegalArgumentException(
                "Duplicate idempotency_key '" + idempotencyKey + "' already used by non-terminal task "
                    + active.get(0).getId()
            );
        }
    }

    @Transactional(readOnly = true)
    public boolean shouldSkipExecution(Task task) {
        if (task == null) {
            return true;
        }
        if (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
            return true;
        }
        if (task.getIdempotencyKey() == null || task.getIdempotencyKey().isBlank()) {
            return false;
        }
        Optional<IdempotencyOutcome> existing = outcomeRepository.findByIdempotencyKey(task.getIdempotencyKey());
        return existing.isPresent();
    }

    @Transactional
    public void recordCompletion(Task task, TaskResult result) {
        if (task == null || task.getIdempotencyKey() == null || task.getIdempotencyKey().isBlank()) {
            return;
        }
        if (task.getStatus() != TaskStatus.SUCCEEDED && task.getStatus() != TaskStatus.DEAD_LETTERED) {
            return;
        }
        if (outcomeRepository.existsByIdempotencyKey(task.getIdempotencyKey())) {
            return;
        }
        IdempotencyOutcome outcome = new IdempotencyOutcome();
        outcome.setIdempotencyKey(task.getIdempotencyKey());
        outcome.setTaskId(task.getId());
        outcome.setWorkflowId(task.getWorkflow() == null ? null : task.getWorkflow().getId());
        outcome.setStatus(task.getStatus().name());
        outcome.setResult(result == null ? null : result.name());
        outcome.setCompletedAt(Instant.now());
        outcomeRepository.save(outcome);
    }
}
