package com.relay.core.service;

import com.relay.core.model.Task;
import org.springframework.stereotype.Component;

@Component
public class RetryPolicy {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    public int getMaxAttempts(Task task) {
        return DEFAULT_MAX_ATTEMPTS;
    }

    public boolean shouldRetry(Task task, int attemptsCompleted) {
        if (task == null) {
            return false;
        }
        return attemptsCompleted < getMaxAttempts(task);
    }
}
