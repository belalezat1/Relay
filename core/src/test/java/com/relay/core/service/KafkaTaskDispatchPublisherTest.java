package com.relay.core.service;

import com.relay.core.model.OutboxEvent;
import com.relay.core.model.Task;
import com.relay.core.model.Workflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaTaskDispatchPublisherTest {

    @Mock
    private OutboxService outboxService;

    @Test
    void enqueuesTaskDispatchKeyedByWorkflowId() {
        KafkaTaskDispatchPublisher publisher = new KafkaTaskDispatchPublisher(outboxService);

        Task task = new Task();
        task.setId(UUID.randomUUID());
        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID());
        task.setWorkflow(workflow);
        task.setType("success");
        task.setIdempotencyKey("task-1");

        OutboxEvent saved = new OutboxEvent();
        saved.setId(UUID.randomUUID());
        when(outboxService.enqueueTaskDispatch(eq(workflow.getId()), any())).thenReturn(saved);

        publisher.publish(task);

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).enqueueTaskDispatch(eq(workflow.getId()), payloadCaptor.capture());
        assertThat(payloadCaptor.getValue()).isInstanceOf(TaskDispatchMessage.class);
        assertThat(((TaskDispatchMessage) payloadCaptor.getValue()).getTaskId()).isEqualTo(task.getId());
    }

    @Test
    void isEnabledWhenKafkaPublisherBeanIsActive() {
        KafkaTaskDispatchPublisher publisher = new KafkaTaskDispatchPublisher(outboxService);
        assertThat(publisher.isEnabled()).isTrue();
    }
}
