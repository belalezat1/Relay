package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;

import java.util.List;
import java.util.Map;

public interface TaskAdapter {
    String adapterType();

    default String label() {
        return adapterType();
    }

    default String description() {
        return "Executes tasks using the " + adapterType() + " adapter.";
    }

    default boolean supportsRetry() {
        return true;
    }

    default boolean supportsAsync() {
        return false;
    }

    default List<String> capabilities() {
        return List.of("execute");
    }

    default boolean supports(String type) {
        return adapterType().equalsIgnoreCase(type);
    }

    TaskResult execute(Task task);

    default Map<String, Object> metadata() {
        return Map.of(
            "type", adapterType(),
            "label", label(),
            "description", description(),
            "supportsRetry", supportsRetry(),
            "supportsAsync", supportsAsync(),
            "capabilities", capabilities()
        );
    }
}
