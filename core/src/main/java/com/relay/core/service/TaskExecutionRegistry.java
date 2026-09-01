package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Component
public class TaskExecutionRegistry {

    private final Map<String, TaskExecutor> executors = new HashMap<>();

    public TaskExecutionRegistry() {
        this(new ObjectMapper(), null);
    }

    public TaskExecutionRegistry(ObjectMapper objectMapper) {
        this(objectMapper, null);
    }

    public TaskExecutionRegistry(ObjectMapper objectMapper, JdbcTemplate jdbcTemplate) {
        register("success", task -> TaskResult.SUCCESS);
        register("charge_card", task -> TaskResult.SUCCESS);
        register("reserve_inventory", task -> TaskResult.SUCCESS);
        register("send_email", task -> TaskResult.SUCCESS);
        register("generate_report", task -> TaskResult.SUCCESS);
        register("fail", task -> {
            throw new IllegalStateException("Task execution failed for type: " + task.getType());
        });
        register("failing_task", task -> {
            throw new IllegalStateException("Task execution failed for type: " + task.getType());
        });

        if (objectMapper != null) {
            registerAdapter(new HttpTaskAdapter(objectMapper));
            if (jdbcTemplate != null) {
                registerAdapter(new DatabaseTaskAdapter(jdbcTemplate, objectMapper));
            }
        }
    }

    public void register(String type, TaskExecutor executor) {
        executors.put(normalize(type), executor);
    }

    public void registerAdapter(TaskAdapter adapter) {
        if (adapter == null) {
            return;
        }
        executors.put(normalize(adapter.adapterType()), adapter::execute);
    }

    public TaskExecutor resolve(Task task) {
        if (task == null) {
            return task1 -> TaskResult.SUCCESS;
        }

        String adapterType = task.getAdapterType();
        if (adapterType != null && !adapterType.isBlank()) {
            TaskExecutor adapterExecutor = executors.get(normalize(adapterType));
            if (adapterExecutor != null) {
                return adapterExecutor;
            }
        }

        return resolve(task.getType());
    }

    public TaskExecutor resolve(String type) {
        String normalized = normalize(type);
        TaskExecutor executor = executors.get(normalized);
        if (executor != null) {
            return executor;
        }

        if (normalized.contains("fail")) {
            return task -> {
                throw new IllegalStateException("Task execution failed for type: " + task.getType());
            };
        }

        return task -> TaskResult.SUCCESS;
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
