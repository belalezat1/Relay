package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.Workflow;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Enqueues workflow lifecycle events into the transactional outbox.
 * {@link OutboxPublisher} publishes to Kafka asynchronously after commit.
 */
@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaWorkflowEventPublisher implements WorkflowEventPublisher {

    private final OutboxService outboxService;

    @Value("${relay.kafka.topic:relay.workflow.events}")
    private String topic;

    public KafkaWorkflowEventPublisher(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @Override
    public void publish(String eventType, Workflow workflow, UUID taskId, String message, Map<String, Object> metadata) {
        if (workflow == null || workflow.getId() == null) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventType", eventType);
        payload.put("workflowId", workflow.getId().toString());
        payload.put("taskId", taskId == null ? null : taskId.toString());
        payload.put("message", message);
        payload.put("metadata", metadata == null ? Map.of() : metadata);
        payload.put("timestamp", Instant.now().toString());
        payload.put("topic", topic);

        outboxService.enqueueWorkflowEvent(workflow.getId(), payload);
    }
}
