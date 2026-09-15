package com.relay.core.repository;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {
    List<Task> findByWorkflow_Id(UUID workflowId);

    List<Task> findByWorkflow_IdOrderByCreatedAtAsc(UUID workflowId);

    List<Task> findByWorkflow_IdAndStatus(UUID workflowId, TaskStatus status);

    List<Task> findByStatus(TaskStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select task from Task task where task.id = :taskId")
    java.util.Optional<Task> findByIdForUpdate(@Param("taskId") UUID taskId);

    @Query("select task from Task task join fetch task.workflow where task.id = :taskId")
    java.util.Optional<Task> findByIdWithWorkflow(@Param("taskId") UUID taskId);

    @Query("""
        select task from Task task
        where task.status = com.relay.core.model.TaskStatus.RUNNING
          and task.executionClaimedAt is not null
          and task.executionClaimedAt < :expiredBefore
        """)
    List<Task> findExpiredRunningClaims(@Param("expiredBefore") java.time.Instant expiredBefore);

    @Query("""
        select task from Task task
        where task.status = com.relay.core.model.TaskStatus.QUEUED
          and task.updatedAt < :staleBefore
        """)
    List<Task> findStaleQueued(@Param("staleBefore") java.time.Instant staleBefore);

    @Query("""
        select task from Task task
        where task.idempotencyKey = :idempotencyKey
          and task.status not in (
            com.relay.core.model.TaskStatus.SUCCEEDED,
            com.relay.core.model.TaskStatus.DEAD_LETTERED
          )
        """)
    List<Task> findNonTerminalByIdempotencyKey(@Param("idempotencyKey") String idempotencyKey);

    @Query(value = """
        select id from tasks
        where status in ('PENDING', 'QUEUED')
          and (next_attempt_at is null or next_attempt_at <= :now)
        order by created_at asc
        limit :limit
        for update skip locked
        """, nativeQuery = true)
    List<UUID> findClaimCandidateIdsSkipLocked(
        @Param("now") java.time.Instant now,
        @Param("limit") int limit
    );

    @Query(value = """
        select id from tasks
        where workflow_id = :workflowId
          and status in ('PENDING', 'QUEUED')
          and (next_attempt_at is null or next_attempt_at <= :now)
        order by created_at asc
        limit :limit
        for update skip locked
        """, nativeQuery = true)
    List<UUID> findClaimCandidateIdsSkipLockedForWorkflow(
        @Param("now") java.time.Instant now,
        @Param("workflowId") UUID workflowId,
        @Param("limit") int limit
    );

    @Query("""
        select task.id from Task task
        where task.status in (
            com.relay.core.model.TaskStatus.PENDING,
            com.relay.core.model.TaskStatus.QUEUED
          )
          and (task.nextAttemptAt is null or task.nextAttemptAt <= :now)
          and (:workflowId is null or task.workflow.id = :workflowId)
        order by task.createdAt asc
        """)
    List<UUID> findClaimCandidateIds(
        @Param("now") java.time.Instant now,
        @Param("workflowId") UUID workflowId
    );

    @Modifying
    @Query("""
        update Task task
        set task.status = com.relay.core.model.TaskStatus.QUEUED,
            task.updatedAt = :now
        where task.id = :taskId
          and task.status = com.relay.core.model.TaskStatus.PENDING
        """)
    int markQueuedIfPending(@Param("taskId") UUID taskId, @Param("now") java.time.Instant now);
}
