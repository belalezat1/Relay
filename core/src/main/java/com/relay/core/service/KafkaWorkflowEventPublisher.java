package com.relay.core.service;

import com.relay.core.model.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true")
public class KafkaWorkflowEventPublisher implements WorkflowEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaWorkflowEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${relay.kafka.topic:relay.workflow.events}")
    private String topic;

    public KafkaWorkflowEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
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

        kafkaTemplate.send(topic, workflow.getId().toString(), payload)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.warn("Failed to publish workflow event {} to Kafka topic {}", eventType, topic, ex);
                }
            });
    }
}
