package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.repository.TaskRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
public class TaskExecutionGuard {

    public enum ClaimDecision {
        CLAIMED,
        ALREADY_COMPLETE,
        ACTIVE_LEASE,
        MISSING
    }

    private final TaskRepository taskRepository;
    private final TaskClaimService taskClaimService;
    private final Duration claimLease;

    public TaskExecutionGuard(
        TaskRepository taskRepository,
        TaskClaimService taskClaimService,
        @Value("${relay.task.claim-lease-seconds:300}") long claimLeaseSeconds
    ) {
        this.taskRepository = taskRepository;
        this.taskClaimService = taskClaimService;
        this.claimLease = Duration.ofSeconds(Math.max(1L, claimLeaseSeconds));
    }

    public Duration getClaimLease() {
        return claimLease;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean claim(UUID taskId) {
        return decide(taskId) == ClaimDecision.CLAIMED;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ClaimDecision decide(UUID taskId) {
        if (taskClaimService != null) {
            return taskClaimService.claim(taskId);
        }
        return legacyDecide(taskId);
    }

    private ClaimDecision legacyDecide(UUID taskId) {
        Task task = taskRepository.findByIdForUpdate(taskId).orElse(null);
        if (task == null) {
            return ClaimDecision.MISSING;
        }
        if (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
            return ClaimDecision.ALREADY_COMPLETE;
        }

        Instant claimedAt = task.getExecutionClaimedAt();
        if (task.getStatus() == TaskStatus.RUNNING
            && claimedAt != null
            && claimedAt.plus(claimLease).isAfter(Instant.now())) {
            return ClaimDecision.ACTIVE_LEASE;
        }

        Instant now = Instant.now();
        task.setStatus(TaskStatus.RUNNING);
        task.setExecutionClaimedAt(now);
        task.setLeaseExpiresAt(now.plus(claimLease));
        taskRepository.saveAndFlush(task);
        return ClaimDecision.CLAIMED;
    }
}
