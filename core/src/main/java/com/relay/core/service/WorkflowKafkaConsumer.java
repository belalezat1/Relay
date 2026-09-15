package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.WorkflowEventProjection;
import com.relay.core.repository.WorkflowEventProjectionRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Projects workflow lifecycle events into Micrometer counters and a durable projection
 * table for operator visibility. Postgres remains the source of truth for workflow state;
 * these projections are secondary telemetry only.
 */
@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class WorkflowKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkflowKafkaConsumer.class);

    private final WorkflowEventProjectionRepository projectionRepository;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final Map<String, AtomicLong> inMemoryCounts = new ConcurrentHashMap<>();

    public WorkflowKafkaConsumer(
        WorkflowEventProjectionRepository projectionRepository,
        ObjectMapper objectMapper,
        @Autowired(required = false) MeterRegistry meterRegistry
    ) {
        this.projectionRepository = projectionRepository;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(
        topics = "${relay.kafka.topic:relay.workflow.events}",
        groupId = "${relay.kafka.consumer.group-id:relay-workflow-events}"
    )
    @Transactional
    public void consume(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return;
        }

        String eventType = String.valueOf(payload.getOrDefault("eventType", "unknown"));
        UUID workflowId = parseUuid(payload.get("workflowId"));
        UUID taskId = parseUuid(payload.get("taskId"));
        String message = payload.get("message") == null ? null : String.valueOf(payload.get("message"));

        inMemoryCounts.computeIfAbsent(eventType, key -> new AtomicLong()).incrementAndGet();
        if (meterRegistry != null) {
            meterRegistry.counter("relay.workflow.events.consumed", "eventType", eventType).increment();
        }

        if (workflowId != null) {
            WorkflowEventProjection projection = new WorkflowEventProjection();
            projection.setEventType(eventType);
            projection.setWorkflowId(workflowId);
            projection.setTaskId(taskId);
            projection.setMessage(message);
            try {
                Object metadata = payload.get("metadata");
                projection.setMetadata(metadata == null ? "{}" : objectMapper.writeValueAsString(metadata));
            } catch (Exception ex) {
                projection.setMetadata("{}");
            }
            projectionRepository.save(projection);
        }

        log.debug("Projected workflow event {} for workflow {} (Postgres remains SoT)", eventType, workflowId);
    }

    public long countForEventType(String eventType) {
        AtomicLong counter = inMemoryCounts.get(eventType);
        return counter == null ? 0L : counter.get();
    }

    private UUID parseUuid(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return UUID.fromString(String.valueOf(value));
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
