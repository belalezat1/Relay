package com.relay.core.repository;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {
    List<Task> findByWorkflow_Id(UUID workflowId);

    List<Task> findByWorkflow_IdAndStatus(UUID workflowId, TaskStatus status);

    List<Task> findByStatus(TaskStatus status);
}
