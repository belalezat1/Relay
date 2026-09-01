package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;

public interface TaskAdapter {
    String adapterType();

    default boolean supports(String type) {
        return adapterType().equalsIgnoreCase(type);
    }

    TaskResult execute(Task task);
}
