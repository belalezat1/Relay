package com.relay.core.repository;

import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface WorkflowRepository extends JpaRepository<Workflow, UUID> {
    List<Workflow> findByStatus(WorkflowStatus status);

    Long countByStatus(WorkflowStatus status);

    List<Workflow> findAllByOrderByCreatedAtDesc();
}
