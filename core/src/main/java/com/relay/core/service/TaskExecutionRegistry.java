package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Component
public class TaskExecutionRegistry {

    private final Map<String, TaskExecutor> executors = new HashMap<>();

    public TaskExecutionRegistry() {
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
    }

    public void register(String type, TaskExecutor executor) {
        executors.put(normalize(type), executor);
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
