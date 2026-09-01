package com.relay.core.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class HttpTaskAdapter implements TaskAdapter {
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper;

    public HttpTaskAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String adapterType() {
        return "http";
    }

    @Override
    public TaskResult execute(Task task) {
        try {
            Map<String, Object> payload = parsePayload(task.getPayload());
            String url = String.valueOf(payload.getOrDefault("url", ""));
            String method = String.valueOf(payload.getOrDefault("method", "GET")).toUpperCase();
            if (url.isBlank()) {
                throw new IllegalArgumentException("HTTP task requires a payload.url value");
            }

            HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url));
            if ("POST".equals(method) || "PUT".equals(method) || "PATCH".equals(method)) {
                String body = payload.get("body") == null ? "" : objectMapper.writeValueAsString(payload.get("body"));
                builder.method(method, HttpRequest.BodyPublishers.ofString(body));
            } else {
                builder.method(method, HttpRequest.BodyPublishers.noBody());
            }

            HttpResponse<String> response = CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return response.statusCode() >= 200 && response.statusCode() < 300 ? TaskResult.SUCCESS : TaskResult.FAILURE;
        } catch (Exception ex) {
            throw new IllegalStateException("HTTP task failed: " + ex.getMessage(), ex);
        }
    }

    private Map<String, Object> parsePayload(String payload) throws Exception {
        if (payload == null || payload.isBlank() || "{}".equals(payload)) {
            return Map.of();
        }
        return objectMapper.readValue(payload, new TypeReference<>() {});
    }
}
