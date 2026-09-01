package com.relay.core.service;

import com.relay.core.model.Workflow;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
@ConditionalOnMissingBean(WorkflowEventPublisher.class)
public class NoOpWorkflowEventPublisher implements WorkflowEventPublisher {

    @Override
    public void publish(String eventType, Workflow workflow, UUID taskId, String message, Map<String, Object> metadata) {
        // Intentionally no-op unless Kafka transport is enabled.
    }
}
