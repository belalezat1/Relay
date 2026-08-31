package com.relay.core.repository;

import com.relay.core.model.TaskAttempt;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface TaskAttemptRepository extends JpaRepository<TaskAttempt, UUID> {
    List<TaskAttempt> findByTask_Id(UUID taskId);
}
