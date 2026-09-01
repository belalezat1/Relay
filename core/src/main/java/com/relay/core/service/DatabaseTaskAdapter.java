package com.relay.core.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

public class DatabaseTaskAdapter implements TaskAdapter {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public DatabaseTaskAdapter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public String adapterType() {
        return "db";
    }

    @Override
    public TaskResult execute(Task task) {
        try {
            Map<String, Object> payload = parsePayload(task.getPayload());
            String sql = String.valueOf(payload.getOrDefault("sql", ""));
            if (sql.isBlank()) {
                throw new IllegalArgumentException("Database task requires a payload.sql value");
            }

            jdbcTemplate.execute(sql);
            return TaskResult.SUCCESS;
        } catch (Exception ex) {
            throw new IllegalStateException("Database task failed: " + ex.getMessage(), ex);
        }
    }

    private Map<String, Object> parsePayload(String payload) throws Exception {
        if (payload == null || payload.isBlank() || "{}".equals(payload)) {
            return Map.of();
        }
        return objectMapper.readValue(payload, new TypeReference<>() {});
    }
}
