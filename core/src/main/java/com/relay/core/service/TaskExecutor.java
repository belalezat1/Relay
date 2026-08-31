package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;

@FunctionalInterface
public interface TaskExecutor {
    TaskResult execute(Task task);
}
