package com.relay.core.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;

import java.util.List;
import java.util.Map;

public class NotificationTaskAdapter implements TaskAdapter {
    private final ObjectMapper objectMapper;

    public NotificationTaskAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String adapterType() {
        return "notification";
    }

    @Override
    public String label() {
        return "Notification";
    }

    @Override
    public String description() {
        return "Sends a lightweight notification-style task payload for operator or user updates.";
    }

    @Override
    public boolean supportsRetry() {
        return true;
    }

    @Override
    public boolean supportsAsync() {
        return true;
    }

    @Override
    public List<String> capabilities() {
        return List.of("execute", "notify", "audit");
    }

    @Override
    public TaskResult execute(Task task) {
        try {
            Map<String, Object> payload = parsePayload(task.getPayload());
            String message = String.valueOf(payload.getOrDefault("message", ""));
            if (message.isBlank()) {
                throw new IllegalArgumentException("Notification task requires a payload.message value");
            }
            return TaskResult.SUCCESS;
        } catch (Exception ex) {
            throw new IllegalStateException("Notification task failed: " + ex.getMessage(), ex);
        }
    }

    private Map<String, Object> parsePayload(String payload) throws Exception {
        if (payload == null || payload.isBlank() || "{}".equals(payload)) {
            return Map.of();
        }
        return objectMapper.readValue(payload, new TypeReference<>() {});
    }
}
