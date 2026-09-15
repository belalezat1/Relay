package com.relay.core.repository;

import com.relay.core.model.KafkaDispatchFailure;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface KafkaDispatchFailureRepository extends JpaRepository<KafkaDispatchFailure, UUID> {
    List<KafkaDispatchFailure> findAllByOrderByCreatedAtDesc();
}
