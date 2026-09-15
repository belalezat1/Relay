package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.OutboxEvent;
import com.relay.core.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(
            outboxEventRepository,
            kafkaTemplate,
            new ObjectMapper(),
            null,
            "relay.workflow.events",
            "relay.workflow.tasks",
            "relay.workflow.tasks.retry",
            50,
            false,
            3,
            2,
            2,
            0.0
        );
    }

    @Test
    void publishFailureReschedulesPendingInsteadOfStickyFailed() throws Exception {
        OutboxEvent event = pendingEvent(OutboxEvent.EVENT_TASK_DISPATCH);
        when(outboxEventRepository.findReadyForUpdate(any())).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        int published = publisher.publishPending();

        assertThat(published).isZero();
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(OutboxEvent.STATUS_PENDING);
        assertThat(saved.getAttemptCount()).isEqualTo(1);
        assertThat(saved.getNextAttemptAt()).isAfter(Instant.now().minusSeconds(1));
        assertThat(saved.getLastError()).contains("broker down");
    }

    @Test
    void publishExhaustsAttemptsThenMarksFailed() throws Exception {
        OutboxEvent event = pendingEvent(OutboxEvent.EVENT_TASK_DISPATCH);
        event.setAttemptCount(2);
        when(outboxEventRepository.findReadyForUpdate(any())).thenReturn(List.of(event));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("still down")));

        publisher.publishPending();

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OutboxEvent.STATUS_FAILED);
        assertThat(captor.getValue().getAttemptCount()).isEqualTo(3);
    }

    @Test
    void taskRetryEventsPublishToRetryTopic() throws Exception {
        OutboxEvent event = pendingEvent(OutboxEvent.EVENT_TASK_RETRY);
        when(outboxEventRepository.findReadyForUpdate(any())).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("relay.workflow.tasks.retry"), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        int published = publisher.publishPending();

        assertThat(published).isEqualTo(1);
        verify(kafkaTemplate).send(eq("relay.workflow.tasks.retry"), anyString(), any());
        verify(kafkaTemplate, never()).send(eq("relay.workflow.tasks"), anyString(), any());
    }

    private OutboxEvent pendingEvent(String eventType) {
        OutboxEvent event = new OutboxEvent();
        event.setId(UUID.randomUUID());
        event.setAggregateType(OutboxEvent.AGGREGATE_TASK);
        event.setAggregateId(UUID.randomUUID());
        event.setEventType(eventType);
        event.setPayload("{\"taskId\":\"" + UUID.randomUUID() + "\"}");
        event.setStatus(OutboxEvent.STATUS_PENDING);
        event.setAttemptCount(0);
        return event;
    }
}
