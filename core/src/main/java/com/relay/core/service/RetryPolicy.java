package com.relay.core.service;

import com.relay.core.model.Task;
import org.springframework.stereotype.Component;

@Component
public class RetryPolicy {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_SECONDS = 5L;
    public static final long DEFAULT_BACKOFF_MULTIPLIER = 2L;

    private boolean backoffEnabled = false;

    public int getMaxAttempts(Task task) {
        return DEFAULT_MAX_ATTEMPTS;
    }

    public long getRetryDelaySeconds(Task task, int attemptsCompleted) {
        if (!backoffEnabled || task == null) {
            return 0L;
        }
        if (attemptsCompleted <= 1) {
            return 0L;
        }
        long attemptNumber = Math.max(0, attemptsCompleted - 2);
        long exponentialDelay = DEFAULT_INITIAL_BACKOFF_SECONDS * (long) Math.pow(DEFAULT_BACKOFF_MULTIPLIER, attemptNumber);
        return exponentialDelay;
    }

    public java.time.Instant getNextAttemptAt(Task task, int attemptsCompleted) {
        long retryDelaySeconds = getRetryDelaySeconds(task, attemptsCompleted);
        if (retryDelaySeconds <= 0L) {
            return null;
        }
        return java.time.Instant.now().plusSeconds(retryDelaySeconds);
    }

    public boolean shouldRetry(Task task, int attemptsCompleted) {
        if (task == null) {
            return false;
        }
        return attemptsCompleted < getMaxAttempts(task);
    }

    public boolean isBackoffEnabled() {
        return backoffEnabled;
    }

    public void setBackoffEnabled(boolean backoffEnabled) {
        this.backoffEnabled = backoffEnabled;
    }
}
