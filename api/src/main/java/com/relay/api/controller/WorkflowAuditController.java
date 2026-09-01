package com.relay.api.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.api.dto.WorkflowAuditEventResponse;
import com.relay.core.model.Workflow;
import com.relay.core.model.WorkflowAuditEvent;
import com.relay.core.repository.WorkflowAuditEventRepository;
import com.relay.core.repository.WorkflowRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/workflows")
public class WorkflowAuditController {

    private final WorkflowRepository workflowRepository;
    private final WorkflowAuditEventRepository workflowAuditEventRepository;
    private final ObjectMapper objectMapper;

    public WorkflowAuditController(
        WorkflowRepository workflowRepository,
        WorkflowAuditEventRepository workflowAuditEventRepository,
        ObjectMapper objectMapper
    ) {
        this.workflowRepository = workflowRepository;
        this.workflowAuditEventRepository = workflowAuditEventRepository;
        this.objectMapper = objectMapper;
    }

    @GetMapping("/{workflowId}/audit")
    public ResponseEntity<List<WorkflowAuditEventResponse>> getAuditTrail(@PathVariable UUID workflowId) {
        Workflow workflow = workflowRepository.findById(workflowId)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Workflow not found: " + workflowId));

        List<WorkflowAuditEvent> events = workflowAuditEventRepository.findByWorkflow_IdOrderByCreatedAtDesc(workflow.getId());
        List<WorkflowAuditEventResponse> responses = new ArrayList<>();
        for (WorkflowAuditEvent event : events) {
            WorkflowAuditEventResponse response = new WorkflowAuditEventResponse();
            response.setId(event.getId());
            response.setEventType(event.getEventType());
            response.setMessage(event.getMessage());
            response.setMetadata(parseMetadata(event.getMetadata()));
            response.setCreatedAt(event.getCreatedAt());
            responses.add(response);
        }
        return ResponseEntity.ok(responses);
    }

    private Map<String, Object> parseMetadata(String metadata) {
        if (metadata == null || metadata.isBlank() || metadata.equals("{}")) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(metadata, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            return Map.of();
        }
    }
}
