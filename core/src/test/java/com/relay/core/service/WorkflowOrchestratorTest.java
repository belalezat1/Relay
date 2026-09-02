package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskAttempt;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowAuditEvent;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.model.WorkflowTemplate;
import com.relay.core.model.WorkflowTemplateRequest;
import com.relay.core.repository.TaskAttemptRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowAuditEventRepository;
import com.relay.core.repository.WorkflowRepository;
import com.relay.core.repository.WorkflowTemplateRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.boot.autoconfigure.domain.EntityScan;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Import({WorkflowOrchestrator.class, DependencyGraphResolver.class, TaskExecutionRegistry.class, RetryPolicy.class, WorkflowAuditTracker.class, NoOpWorkflowEventPublisher.class, ObjectMapper.class, WorkflowTemplateService.class})
@org.springframework.test.context.ContextConfiguration(classes = WorkflowOrchestratorTest.TestConfiguration.class)
class WorkflowOrchestratorTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableJpaRepositories(basePackageClasses = {WorkflowRepository.class, TaskRepository.class, TaskAttemptRepository.class, WorkflowAuditEventRepository.class, WorkflowTemplateRepository.class})
    @EntityScan(basePackageClasses = {Workflow.class, Task.class, TaskAttempt.class, WorkflowAuditEvent.class, WorkflowTemplate.class})
    static class TestConfiguration {
    }

    @Autowired
    private WorkflowOrchestrator workflowOrchestrator;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskAttemptRepository taskAttemptRepository;

    @Autowired
    private TaskExecutionRegistry taskExecutionRegistry;

    @Autowired
    private WorkflowTemplateRepository workflowTemplateRepository;

    @Autowired
    private WorkflowTemplateService workflowTemplateService;

    @Test
    void createsWorkflowFromTemplate() {
        WorkflowTemplateRequest request = new WorkflowTemplateRequest();
        request.setName("data-sync");
        request.setCategory("data");
        request.setOwner("platform");
        request.setEnvironment("dev");

        TaskDefinition extract = new TaskDefinition();
        extract.setId("extract");
        extract.setType("success");
        extract.setPayload(Map.of("source", "warehouse"));

        TaskDefinition transform = new TaskDefinition();
        transform.setId("transform");
        transform.setType("success");
        transform.setDependsOn(List.of("extract"));
        transform.setPayload(Map.of("target", "normalized"));

        request.setTasks(List.of(extract, transform));

        WorkflowTemplate template = workflowTemplateService.createTemplate(request);
        Workflow workflow = workflowTemplateService.submitTemplate(template.getId(), "ops", "prod", 120, 300);

        assertThat(workflowTemplateRepository.findById(template.getId())).isPresent();
        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(workflow.getOwner()).isEqualTo("ops");
        assertThat(workflow.getEnvironment()).isEqualTo("prod");
        assertThat(taskRepository.findByWorkflow_Id(workflow.getId())).hasSize(2);
    }

    @Test
    void executesSuccessfulWorkflow() {
        TaskDefinition first = new TaskDefinition();
        first.setId("first");
        first.setType("success");
        first.setPayload(Map.of("message", "hello"));

        TaskDefinition second = new TaskDefinition();
        second.setId("second");
        second.setType("success");
        second.setDependsOn(List.of("first"));

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(first, second));

        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(workflowRepository.findById(workflow.getId())).isPresent();
        assertThat(taskRepository.findByWorkflow_Id(workflow.getId())).hasSize(2);
        assertThat(taskRepository.findByWorkflow_Id(workflow.getId()).stream()
            .filter(task -> task.getStatus() == TaskStatus.SUCCEEDED)
            .count()).isEqualTo(2);
    }

    @Test
    void failsWorkflowWhenAnyTaskFails() {
        TaskDefinition first = new TaskDefinition();
        first.setId("first");
        first.setType("fail");

        TaskDefinition second = new TaskDefinition();
        second.setId("second");
        second.setType("success");
        second.setDependsOn(List.of("first"));

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(first, second));

        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.FAILED);
        assertThat(taskRepository.findByWorkflow_Id(workflow.getId())).anyMatch(task -> task.getStatus() == TaskStatus.DEAD_LETTERED);
        assertThat(taskAttemptRepository.findAll()).isNotEmpty();
    }

    @Test
    void persistsAttemptMetadata() {
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setId("single");
        taskDefinition.setType("success");

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(taskDefinition));

        assertThat(taskAttemptRepository.findAll()).hasSize(1);
        assertThat(taskAttemptRepository.findAll().getFirst().getResult()).isEqualTo(com.relay.core.model.TaskResult.SUCCESS);
        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @Test
    void retriesTransientFailuresUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();
        taskExecutionRegistry.register("flaky_success", task -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("temporary failure");
            }
            return com.relay.core.model.TaskResult.SUCCESS;
        });

        TaskDefinition workflowTask = new TaskDefinition();
        workflowTask.setId("flaky");
        workflowTask.setType("flaky_success");

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(workflowTask));

        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(attempts.get()).isEqualTo(2);
        Task task = taskRepository.findByWorkflow_Id(workflow.getId()).getFirst();
        assertThat(task.getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void deadLettersTaskAfterRetryLimit() {
        taskExecutionRegistry.register("always_fail", task -> {
            throw new IllegalStateException("permanent failure");
        });

        TaskDefinition workflowTask = new TaskDefinition();
        workflowTask.setId("terminal");
        workflowTask.setType("always_fail");

        Workflow workflow = workflowOrchestrator.createAndExecuteWorkflow(List.of(workflowTask));

        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.FAILED);
        Task task = taskRepository.findByWorkflow_Id(workflow.getId()).getFirst();
        assertThat(task.getStatus()).isEqualTo(TaskStatus.DEAD_LETTERED);
        assertThat(task.getAttemptCount()).isEqualTo(RetryPolicy.DEFAULT_MAX_ATTEMPTS);
    }

    @Test
    void pausesResumesAndCancelsWorkflow() {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.PENDING);
        workflow = workflowRepository.saveAndFlush(workflow);

        workflow = workflowOrchestrator.pauseWorkflow(workflow.getId());
        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.PAUSED);

        workflow = workflowOrchestrator.resumeWorkflow(workflow.getId());
        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.PENDING);

        workflow = workflowOrchestrator.cancelWorkflow(workflow.getId());
        assertThat(workflow.getStatus()).isEqualTo(WorkflowStatus.CANCELLED);
    }

    @Test
    void failsWorkflowWhenTimeoutIsExceeded() {
        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.RUNNING);
        workflow.setCreatedAt(Instant.now().minusSeconds(30));
        workflow.setTimeoutSeconds(1);
        workflow = workflowRepository.saveAndFlush(workflow);

        Workflow updated = workflowOrchestrator.executeWorkflow(workflow.getId());

        assertThat(updated.getStatus()).isEqualTo(WorkflowStatus.FAILED);
    }
}
