package com.relay.core.repository;

import com.relay.core.model.WorkflowAuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface WorkflowAuditEventRepository extends JpaRepository<WorkflowAuditEvent, UUID> {
    List<WorkflowAuditEvent> findByWorkflow_IdOrderByCreatedAtDesc(UUID workflowId);
}
