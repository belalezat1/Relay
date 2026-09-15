package com.relay.core.repository;

import com.relay.core.model.WorkflowEventProjection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface WorkflowEventProjectionRepository extends JpaRepository<WorkflowEventProjection, UUID> {
    List<WorkflowEventProjection> findByWorkflowIdOrderByReceivedAtDesc(UUID workflowId);

    long countByEventType(String eventType);
}
