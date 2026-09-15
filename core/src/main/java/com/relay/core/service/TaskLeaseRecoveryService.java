package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class TaskLeaseRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(TaskLeaseRecoveryService.class);

    private final TaskRepository taskRepository;
    private final TaskExecutionGuard taskExecutionGuard;
    private final WorkflowAuditTracker workflowAuditTracker;

    public TaskLeaseRecoveryService(
        TaskRepository taskRepository,
        TaskExecutionGuard taskExecutionGuard,
        WorkflowAuditTracker workflowAuditTracker
    ) {
        this.taskRepository = taskRepository;
        this.taskExecutionGuard = taskExecutionGuard;
        this.workflowAuditTracker = workflowAuditTracker;
    }

    @Scheduled(fixedDelayString = "${relay.task.lease-recovery-delay:15000}")
    @Transactional
    public int recoverExpiredWork() {
        Instant expiredBefore = Instant.now().minus(taskExecutionGuard.getClaimLease());
        List<Task> recovered = new ArrayList<>();
        recovered.addAll(taskRepository.findExpiredRunningClaims(expiredBefore));
        recovered.addAll(taskRepository.findStaleQueued(expiredBefore));

        for (Task task : recovered) {
            TaskStatus previous = task.getStatus();
            task.setStatus(TaskStatus.PENDING);
            task.setExecutionClaimedAt(null);
            task.setLockedBy(null);
            task.setLeaseExpiresAt(null);
            taskRepository.save(task);
            if (task.getWorkflow() != null) {
                workflowAuditTracker.record(
                    task.getWorkflow(),
                    task.getId(),
                    "task.lease.recovered",
                    "Expired task claim or stale queue entry returned to pending for rediscovery",
                    Map.of(
                        "previousStatus", previous.name(),
                        "status", task.getStatus().name(),
                        "source", "lease-recovery"
                    )
                );
            }
            log.warn("Recovered {} task {} after lease/queue expiry", previous, task.getId());
        }
        return recovered.size();
    }
}
