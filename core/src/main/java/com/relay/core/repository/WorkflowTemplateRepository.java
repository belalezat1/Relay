package com.relay.core.repository;

import com.relay.core.model.WorkflowTemplate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WorkflowTemplateRepository extends JpaRepository<WorkflowTemplate, UUID> {
    Optional<WorkflowTemplate> findByName(String name);

    List<WorkflowTemplate> findAllByOrderByCreatedAtDesc();
}
