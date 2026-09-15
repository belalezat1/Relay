package com.relay.core.repository;

import com.relay.core.model.OutboxEvent;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
        select event from OutboxEvent event
        where event.status = 'PENDING'
          and (event.nextAttemptAt is null or event.nextAttemptAt <= :now)
        order by event.createdAt asc
        """)
    List<OutboxEvent> findReadyForUpdate(@Param("now") Instant now);

    @Query(value = """
        select * from outbox_events
        where status = 'PENDING'
          and (next_attempt_at is null or next_attempt_at <= :now)
        order by created_at asc
        limit :batchSize
        for update skip locked
        """, nativeQuery = true)
    List<OutboxEvent> findReadyForUpdateSkipLocked(@Param("now") Instant now, @Param("batchSize") int batchSize);

    long countByStatus(String status);
}
