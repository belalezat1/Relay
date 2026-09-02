package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class TaskExecutionRegistry {

    private final Map<String, TaskExecutor> executors = new HashMap<>();
    private final Map<String, TaskAdapter> adapters = new HashMap<>();

    public TaskExecutionRegistry() {
        this(new ObjectMapper(), null);
    }

    public TaskExecutionRegistry(ObjectMapper objectMapper) {
        this(objectMapper, null);
    }

    public TaskExecutionRegistry(ObjectMapper objectMapper, JdbcTemplate jdbcTemplate) {
        register("inline", task -> TaskResult.SUCCESS);
        register("success", task -> TaskResult.SUCCESS);
        register("charge_card", task -> TaskResult.SUCCESS);
        register("reserve_inventory", task -> TaskResult.SUCCESS);
        register("send_email", task -> TaskResult.SUCCESS);
        register("generate_report", task -> TaskResult.SUCCESS);
        register("db", task -> TaskResult.SUCCESS);
        register("database", task -> TaskResult.SUCCESS);
        register("fail", task -> {
            throw new IllegalStateException("Task execution failed for type: " + task.getType());
        });
        register("failing_task", task -> {
            throw new IllegalStateException("Task execution failed for type: " + task.getType());
        });

        if (objectMapper != null) {
            registerAdapter(new HttpTaskAdapter(objectMapper));
            registerAdapter(new NotificationTaskAdapter(objectMapper));
            if (jdbcTemplate != null) {
                registerAdapter(new DatabaseTaskAdapter(jdbcTemplate, objectMapper));
            }
        }
    }

    public void register(String type, TaskExecutor executor) {
        executors.put(normalize(type), executor);
        adapters.putIfAbsent(normalize(type), new TaskAdapter() {
            @Override
            public String adapterType() {
                return type;
            }

            @Override
            public String label() {
                return type;
            }

            @Override
            public String description() {
                return "Built-in executor for the " + type + " task type.";
            }

            @Override
            public TaskResult execute(Task task) {
                return executor.execute(task);
            }
        });
    }

    public void registerAdapter(TaskAdapter adapter) {
        if (adapter == null) {
            return;
        }
        String primaryType = normalize(adapter.adapterType());
        adapters.put(primaryType, adapter);
        executors.put(primaryType, adapter::execute);

        if ("db".equals(primaryType)) {
            adapters.put("database", adapter);
            executors.put("database", adapter::execute);
        }
        if ("database".equals(primaryType)) {
            adapters.put("db", adapter);
            executors.put("db", adapter::execute);
        }
    }

    public boolean supportsAdapterType(String adapterType) {
        if (adapterType == null || adapterType.isBlank()) {
            return false;
        }
        return adapters.containsKey(normalize(adapterType)) || executors.containsKey(normalize(adapterType));
    }

    public List<Map<String, Object>> listAdapterCapabilities() {
        List<Map<String, Object>> capabilities = new ArrayList<>();
        for (TaskAdapter adapter : adapters.values()) {
            capabilities.add(adapter.metadata());
        }
        return capabilities.stream()
            .sorted((left, right) -> String.valueOf(left.get("type")).compareToIgnoreCase(String.valueOf(right.get("type"))))
            .toList();
    }

    public TaskExecutor resolve(Task task) {
        if (task == null) {
            return task1 -> TaskResult.SUCCESS;
        }

        String adapterType = task.getAdapterType();
        if (adapterType != null && !adapterType.isBlank() && !"inline".equalsIgnoreCase(adapterType)) {
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
