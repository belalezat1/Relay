package com.relay.core.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TaskRetryConsumerTest {

    @Mock
    private OutboxService outboxService;

    @Test
    void dueRetryPromotesToMainDispatchTopic() {
        TaskRetryConsumer consumer = new TaskRetryConsumer(outboxService);
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        Map<String, Object> payload = new HashMap<>();
        payload.put("workflowId", workflowId.toString());
        payload.put("taskId", taskId.toString());
        payload.put("retryAfter", Instant.now().minusSeconds(5).toString());

        consumer.consume(payload);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(outboxService).enqueueTaskDispatch(eq(workflowId), captor.capture());
        assertThat(captor.getValue()).doesNotContainKey("retryAfter");
        assertThat(captor.getValue().get("taskId")).isEqualTo(taskId.toString());
    }

    @Test
    void earlyRetryRequeuesOntoRetryOutbox() {
        TaskRetryConsumer consumer = new TaskRetryConsumer(outboxService);
        UUID workflowId = UUID.randomUUID();
        Instant retryAfter = Instant.now().plusSeconds(30);
        Map<String, Object> payload = new HashMap<>();
        payload.put("workflowId", workflowId.toString());
        payload.put("taskId", UUID.randomUUID().toString());
        payload.put("retryAfter", retryAfter.toString());

        consumer.consume(payload);

        verify(outboxService).enqueueTaskRetry(eq(workflowId), any(), eq(retryAfter));
    }
}
