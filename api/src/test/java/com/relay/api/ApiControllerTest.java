package com.relay.api;

import com.relay.core.model.DeadLetterTask;
import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowStatus;
import com.relay.core.repository.DeadLetterTaskRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.repository.WorkflowRepository;
import com.relay.core.service.DeadLetterTaskService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = RelayApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
    "spring.datasource.url=jdbc:h2:mem:relay_api;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.jpa.hibernate.ddl-auto=create-drop",
    "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
    "spring.flyway.enabled=false",
    "APP_ENV=test",
    "spring.profiles.active=test",
    "relay.kafka.enabled=false",
    "KAFKA_ENABLED=false",
    "spring.kafka.listener.auto-startup=false",
    "management.health.kafka.enabled=false",
    "relay.worker.poll-delay=3600000",
    "relay.task.lease-recovery-delay=3600000",
    "relay.outbox.poll-delay=3600000",
    "relay.kafka.lag-poll-delay=3600000",
    "relay.worker.orchestration-enabled=true",
    "relay.retry.backoff-enabled=false",
    "relay.task.claim.skip-locked=false",
    "logging.level.org.hibernate.SQL=WARN",
    "logging.level.com.relay=INFO"
})
class ApiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private WorkflowRepository workflowRepository;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private DeadLetterTaskRepository deadLetterTaskRepository;

    @Autowired
    private DeadLetterTaskService deadLetterTaskService;

    @Test
    void healthEndpointIsUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void submitDependentWorkflowAndLifecycleControls() throws Exception {
        MvcResult created = mockMvc.perform(post("/workflows")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    {
                      "tasks": [
                        {"id": "task-a", "type": "success"},
                        {"id": "task-b", "type": "success", "dependsOn": ["task-a"]}
                      ]
                    }
                    """))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.status").value("COMPLETED"))
            .andExpect(jsonPath("$.tasks.length()").value(2))
            .andReturn();

        String body = created.getResponse().getContentAsString();
        String workflowId = body.replaceAll("(?s)^\\{\"id\"\\s*:\\s*\"([^\"]+)\".*", "$1");

        mockMvc.perform(get("/workflows/" + workflowId))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("COMPLETED"));

        Workflow pausedSeed = new Workflow();
        pausedSeed.setStatus(WorkflowStatus.PENDING);
        pausedSeed = workflowRepository.saveAndFlush(pausedSeed);

        mockMvc.perform(post("/workflows/" + pausedSeed.getId() + "/pause"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("PAUSED"));

        mockMvc.perform(post("/workflows/" + pausedSeed.getId() + "/resume"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("PENDING"));

        mockMvc.perform(post("/workflows/" + pausedSeed.getId() + "/cancel"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void listDeadLettersReplayAndDispatchFailures() throws Exception {
        mockMvc.perform(get("/dispatch-failures"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.length()").value(0));

        Workflow workflow = new Workflow();
        workflow.setStatus(WorkflowStatus.FAILED);
        workflow = workflowRepository.saveAndFlush(workflow);

        Task task = new Task();
        task.setType("fail");
        task.setStatus(TaskStatus.DEAD_LETTERED);
        task.setAttemptCount(3);
        task.setDependsOn(new UUID[0]);
        workflow.addTask(task);
        task = taskRepository.saveAndFlush(task);

        DeadLetterTask deadLetter = deadLetterTaskService.record(task, "terminal failure");
        assertThat(deadLetter).isNotNull();
        UUID deadLetterId = deadLetter.getId();

        mockMvc.perform(get("/dead-letters"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.length()", greaterThanOrEqualTo(1)))
            .andExpect(jsonPath("$[0].error").value("terminal failure"));

        mockMvc.perform(post("/dead-letters/" + deadLetterId + "/replay"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.taskStatus").value("PENDING"))
            .andExpect(jsonPath("$.attemptCount").value(0))
            .andExpect(jsonPath("$.intervention").value("replayed"));

        Task reloaded = taskRepository.findById(task.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(reloaded.getAttemptCount()).isZero();
        assertThat(deadLetterTaskRepository.findById(deadLetterId)).isEmpty();

        Workflow reopened = workflowRepository.findById(workflow.getId()).orElseThrow();
        assertThat(reopened.getStatus()).isEqualTo(WorkflowStatus.PENDING);
    }
}
