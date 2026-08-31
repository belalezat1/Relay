package com.relay.core.repository;

import com.relay.core.model.Task;
import com.relay.core.model.TaskAttempt;
import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@TestPropertySource(properties = {
    "spring.datasource.url=jdbc:h2:mem:relay;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "spring.jpa.properties.hibernate.format_sql=true",
    "spring.flyway.enabled=false"
})
class DatabasePersistenceSmokeTest {

    @SpringBootApplication
    @EntityScan(basePackages = "com.relay.core.model")
    @EnableJpaRepositories(basePackages = "com.relay.core.repository")
    static class TestApplication {
    }

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskAttemptRepository taskAttemptRepository;

    @Test
    void persistWorkflowAndTaskGraph() {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.PENDING);

        Task task = new Task();
        task.setType("charge_card");
        task.setPayload("{\"amount\":100,\"currency\":\"USD\"}");
        task.setStatus(TaskStatus.PENDING);
        task.setDependsOn(new UUID[0]);
        task.setAttemptCount(0);
        task.setIdempotencyKey("charge-card-1");
        workflow.addTask(task);

        Workflow savedWorkflow = workflowRepository.saveAndFlush(workflow);

        assertThat(savedWorkflow.getId()).isNotNull();
        List<Task> tasks = taskRepository.findByWorkflow_Id(savedWorkflow.getId());
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).getType()).isEqualTo("charge_card");
        assertThat(tasks.get(0).getPayload()).contains("amount");

        Task savedTask = tasks.get(0);
        TaskAttempt attempt = new TaskAttempt();
        attempt.setTask(savedTask);
        attempt.setStartedAt(Instant.now());
        attempt.setFinishedAt(Instant.now());
        attempt.setResult(TaskResult.SUCCESS);
        attempt.setError(null);

        TaskAttempt savedAttempt = taskAttemptRepository.saveAndFlush(attempt);
        assertThat(savedAttempt.getId()).isNotNull();
        assertThat(taskAttemptRepository.findByTask_Id(savedTask.getId())).hasSize(1);

        Workflow loadedWorkflow = workflowRepository.findById(savedWorkflow.getId()).orElseThrow();
        assertThat(loadedWorkflow.getStatus()).isEqualTo(WorkflowStatus.PENDING);
    }
}
