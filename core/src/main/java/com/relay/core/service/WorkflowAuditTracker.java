package com.relay.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowAuditEvent;
import com.relay.core.repository.WorkflowAuditEventRepository;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
public class WorkflowAuditTracker {

    private final WorkflowAuditEventRepository repository;
    private final ObjectMapper objectMapper;

    public WorkflowAuditTracker(WorkflowAuditEventRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public void record(Workflow workflow, String eventType, String message) {
        record(workflow, null, eventType, message, Map.of());
    }

    public void record(Workflow workflow, UUID taskId, String eventType, String message, Map<String, Object> metadata) {
        WorkflowAuditEvent event = new WorkflowAuditEvent();
        event.setWorkflow(workflow);
        event.setTaskId(taskId);
        event.setEventType(eventType);
        event.setMessage(message);
        try {
            event.setMetadata(objectMapper.writeValueAsString(metadata == null ? Map.of() : metadata));
        } catch (JsonProcessingException e) {
            event.setMetadata("{}");
        }
        repository.save(event);
    }
}
