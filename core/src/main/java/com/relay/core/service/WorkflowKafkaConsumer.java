package com.relay.core.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true")
public class WorkflowKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkflowKafkaConsumer.class);

    @KafkaListener(
        topics = "${relay.kafka.topic:relay.workflow.events}",
        groupId = "${relay.kafka.consumer.group-id:relay-workflow-events}"
    )
    public void consume(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return;
        }
        log.info("Received workflow event payload: {}", payload);
    }
}
