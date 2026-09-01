package com.relay.core.service;

import com.relay.core.model.Workflow;

import java.util.Map;
import java.util.UUID;

public interface WorkflowEventPublisher {
    void publish(String eventType, Workflow workflow, UUID taskId, String message, Map<String, Object> metadata);
}
