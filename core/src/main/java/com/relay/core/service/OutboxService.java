package com.relay.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.OutboxEvent;
import com.relay.core.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Writes Kafka-bound messages into {@code outbox_events} in the same transaction as
 * workflow/task state changes. {@link OutboxPublisher} drains ready unpublished rows.
 */
@Service
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OutboxEvent enqueue(String aggregateType, UUID aggregateId, String eventType, Object payload) {
        return enqueue(aggregateType, aggregateId, eventType, payload, null);
    }

    @Transactional
    public OutboxEvent enqueue(
        String aggregateType,
        UUID aggregateId,
        String eventType,
        Object payload,
        Instant nextAttemptAt
    ) {
        OutboxEvent event = new OutboxEvent();
        event.setAggregateType(aggregateType);
        event.setAggregateId(aggregateId);
        event.setEventType(eventType);
        event.setPayload(toJson(payload));
        event.setStatus(OutboxEvent.STATUS_PENDING);
        event.setAttemptCount(0);
        event.setNextAttemptAt(nextAttemptAt);
        return outboxEventRepository.save(event);
    }

    @Transactional
    public OutboxEvent enqueueWorkflowEvent(UUID workflowId, Map<String, Object> payload) {
        return enqueue(OutboxEvent.AGGREGATE_WORKFLOW, workflowId, OutboxEvent.EVENT_WORKFLOW, payload);
    }

    @Transactional
    public OutboxEvent enqueueTaskDispatch(UUID workflowId, Object payload) {
        return enqueue(OutboxEvent.AGGREGATE_TASK, workflowId, OutboxEvent.EVENT_TASK_DISPATCH, payload);
    }

    @Transactional
    public OutboxEvent enqueueTaskRetry(UUID workflowId, Object payload, Instant retryAfter) {
        return enqueue(
            OutboxEvent.AGGREGATE_TASK,
            workflowId,
            OutboxEvent.EVENT_TASK_RETRY,
            payload,
            retryAfter
        );
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload == null ? Map.of() : payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Unable to serialize outbox payload", ex);
        }
    }
}
