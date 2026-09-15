package com.relay.core.service;

import com.relay.core.model.OutboxEvent;
import com.relay.core.model.Task;
import com.relay.core.model.Workflow;
import com.relay.core.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaWorkflowEventPublisherTest {

    @Mock
    private OutboxService outboxService;

    @Test
    void enqueuesWorkflowLifecycleEventToOutbox() {
        KafkaWorkflowEventPublisher publisher = new KafkaWorkflowEventPublisher(outboxService);
        ReflectionTestUtils.setField(publisher, "topic", "relay.workflow.events");

        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID());
        UUID taskId = UUID.randomUUID();
        OutboxEvent saved = new OutboxEvent();
        saved.setId(UUID.randomUUID());
        when(outboxService.enqueueWorkflowEvent(eq(workflow.getId()), any())).thenReturn(saved);

        publisher.publish("workflow.created", workflow, taskId, "Workflow created", Map.of("owner", "platform"));

        ArgumentCaptor<Map<String, Object>> payloadCaptor = ArgumentCaptor.forClass(Map.class);
        verify(outboxService).enqueueWorkflowEvent(eq(workflow.getId()), payloadCaptor.capture());
        assertThat(payloadCaptor.getValue().get("eventType")).isEqualTo("workflow.created");
        assertThat(payloadCaptor.getValue().get("workflowId")).isEqualTo(workflow.getId().toString());
    }
}
