package com.relay.core.repository;

import com.relay.core.model.DeadLetterTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface DeadLetterTaskRepository extends JpaRepository<DeadLetterTask, UUID> {
    @Query("select deadLetter from DeadLetterTask deadLetter join fetch deadLetter.task task order by deadLetter.createdAt desc")
    List<DeadLetterTask> findAllByOrderByCreatedAtDesc();

    @Query("select deadLetter from DeadLetterTask deadLetter join fetch deadLetter.task task where deadLetter.workflowId = :workflowId order by deadLetter.createdAt desc")
    List<DeadLetterTask> findByWorkflowIdOrderByCreatedAtDesc(@Param("workflowId") UUID workflowId);

    boolean existsByTask_Id(UUID taskId);

    boolean existsByTask_IdAndReplayedAtIsNull(UUID taskId);

    @Query("select deadLetter from DeadLetterTask deadLetter join fetch deadLetter.task task where deadLetter.id = :id")
    java.util.Optional<DeadLetterTask> findByIdWithTask(@Param("id") UUID id);
}
