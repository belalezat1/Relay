package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.repository.TaskRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Claims ready PENDING/QUEUED tasks for execution.
 * <p>
 * On PostgreSQL, uses {@code FOR UPDATE SKIP LOCKED} so concurrent workers do not
 * double-claim the same row. On H2 (unit tests), falls back to pessimistic
 * {@code FOR UPDATE} via {@link TaskRepository#findByIdForUpdate(UUID)}.
 */
@Service
public class TaskClaimService {

    private static final Logger log = LoggerFactory.getLogger(TaskClaimService.class);

    private final TaskRepository taskRepository;
    private final Duration claimLease;
    private final String workerId;
    private final boolean skipLockedEnabled;

    @PersistenceContext
    private EntityManager entityManager;

    public TaskClaimService(
        TaskRepository taskRepository,
        @Value("${relay.task.claim-lease-seconds:300}") long claimLeaseSeconds,
        @Value("${relay.worker.id:}") String configuredWorkerId,
        @Value("${relay.task.claim.skip-locked:true}") boolean skipLockedEnabled
    ) {
        this.taskRepository = taskRepository;
        this.claimLease = Duration.ofSeconds(Math.max(1L, claimLeaseSeconds));
        this.workerId = configuredWorkerId == null || configuredWorkerId.isBlank()
            ? "worker-" + UUID.randomUUID()
            : configuredWorkerId.trim();
        this.skipLockedEnabled = skipLockedEnabled;
    }

    public String getWorkerId() {
        return workerId;
    }

    public Duration getClaimLease() {
        return claimLease;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public TaskExecutionGuard.ClaimDecision claim(UUID taskId) {
        return claimInternal(taskId, workerId);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public TaskExecutionGuard.ClaimDecision claim(UUID taskId, String claimant) {
        return claimInternal(taskId, claimant == null || claimant.isBlank() ? workerId : claimant);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<Task> claimNextReady(int limit) {
        List<Task> claimed = claimReady(null, Math.max(1, limit));
        return claimed.stream().findFirst();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Task> claimReady(UUID workflowId, int limit) {
        int batch = Math.max(1, limit);
        Instant now = Instant.now();
        List<UUID> candidates = loadCandidates(now, workflowId, batch);
        List<Task> claimed = new ArrayList<>();
        for (UUID taskId : candidates) {
            if (claimInternal(taskId, workerId) == TaskExecutionGuard.ClaimDecision.CLAIMED) {
                taskRepository.findByIdWithWorkflow(taskId).ifPresent(claimed::add);
            }
            if (claimed.size() >= batch) {
                break;
            }
        }
        return claimed;
    }

    @Transactional
    public boolean markQueuedIfPending(UUID taskId) {
        return taskRepository.markQueuedIfPending(taskId, Instant.now()) > 0;
    }

    private List<UUID> loadCandidates(Instant now, UUID workflowId, int limit) {
        if (supportsSkipLocked()) {
            try {
                if (workflowId == null) {
                    return taskRepository.findClaimCandidateIdsSkipLocked(now, limit);
                }
                return taskRepository.findClaimCandidateIdsSkipLockedForWorkflow(now, workflowId, limit);
            } catch (RuntimeException ex) {
                log.debug("SKIP LOCKED claim query unavailable; falling back to FOR UPDATE: {}", ex.getMessage());
            }
        }
        List<UUID> ids = taskRepository.findClaimCandidateIds(now, workflowId);
        return ids.size() <= limit ? ids : ids.subList(0, limit);
    }

    private TaskExecutionGuard.ClaimDecision claimInternal(UUID taskId, String claimant) {
        Task task = taskRepository.findByIdForUpdate(taskId).orElse(null);
        if (task == null) {
            return TaskExecutionGuard.ClaimDecision.MISSING;
        }
        if (task.getStatus() == TaskStatus.SUCCEEDED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
            return TaskExecutionGuard.ClaimDecision.ALREADY_COMPLETE;
        }

        Instant now = Instant.now();
        if (task.getNextAttemptAt() != null && task.getNextAttemptAt().isAfter(now)) {
            return TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE;
        }

        Instant leaseExpiresAt = task.getLeaseExpiresAt();
        if (task.getStatus() == TaskStatus.RUNNING
            && leaseExpiresAt != null
            && leaseExpiresAt.isAfter(now)) {
            return TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE;
        }

        Instant claimedAt = task.getExecutionClaimedAt();
        if (task.getStatus() == TaskStatus.RUNNING
            && claimedAt != null
            && claimedAt.plus(claimLease).isAfter(now)
            && (task.getLockedBy() == null || !task.getLockedBy().equals(claimant))) {
            return TaskExecutionGuard.ClaimDecision.ACTIVE_LEASE;
        }

        task.setStatus(TaskStatus.RUNNING);
        task.setExecutionClaimedAt(now);
        task.setLeaseExpiresAt(now.plus(claimLease));
        task.setLockedBy(claimant);
        taskRepository.saveAndFlush(task);
        return TaskExecutionGuard.ClaimDecision.CLAIMED;
    }

    private boolean supportsSkipLocked() {
        if (!skipLockedEnabled) {
            return false;
        }
        try {
            String product = entityManager
                .unwrap(org.hibernate.Session.class)
                .doReturningWork(connection -> connection.getMetaData().getDatabaseProductName());
            return product != null && product.toLowerCase().contains("postgresql");
        } catch (RuntimeException ex) {
            return false;
        }
    }
}
