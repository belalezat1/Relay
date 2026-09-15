package com.relay.core.repository;

import com.relay.core.model.IdempotencyOutcome;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IdempotencyOutcomeRepository extends JpaRepository<IdempotencyOutcome, String> {
    Optional<IdempotencyOutcome> findByIdempotencyKey(String idempotencyKey);

    boolean existsByIdempotencyKey(String idempotencyKey);
}
