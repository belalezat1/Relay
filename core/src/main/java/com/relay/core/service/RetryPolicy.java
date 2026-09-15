package com.relay.core.service;

import com.relay.core.model.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class RetryPolicy {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_INITIAL_BACKOFF_SECONDS = 5L;
    public static final long DEFAULT_BACKOFF_MULTIPLIER = 2L;
    public static final double DEFAULT_JITTER_FRACTION = 0.2d;

    private boolean backoffEnabled;
    private final int maxAttempts;
    private final long initialBackoffSeconds;
    private final long backoffMultiplier;
    private final double jitterFraction;

    public RetryPolicy() {
        this(true, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_BACKOFF_SECONDS, DEFAULT_BACKOFF_MULTIPLIER, DEFAULT_JITTER_FRACTION);
    }

    @Autowired
    public RetryPolicy(
        @Value("${relay.retry.backoff-enabled:${relay.kafka.retry-backoff-enabled:true}}") boolean backoffEnabled,
        @Value("${relay.retry.max-attempts:3}") int maxAttempts,
        @Value("${relay.retry.initial-backoff-seconds:5}") long initialBackoffSeconds,
        @Value("${relay.retry.backoff-multiplier:2}") long backoffMultiplier,
        @Value("${relay.retry.jitter-fraction:0.2}") double jitterFraction
    ) {
        this.backoffEnabled = backoffEnabled;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoffSeconds = Math.max(0L, initialBackoffSeconds);
        this.backoffMultiplier = Math.max(1L, backoffMultiplier);
        this.jitterFraction = Math.max(0d, Math.min(1d, jitterFraction));
    }

    public int getMaxAttempts(Task task) {
        return maxAttempts;
    }

    public long getRetryDelaySeconds(Task task, int attemptsCompleted) {
        if (!backoffEnabled || task == null) {
            return 0L;
        }
        if (attemptsCompleted <= 1) {
            return 0L;
        }
        long attemptNumber = Math.max(0, attemptsCompleted - 2);
        long exponentialDelay = initialBackoffSeconds * (long) Math.pow(backoffMultiplier, attemptNumber);
        return applyJitter(exponentialDelay);
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

    public int getConfiguredMaxAttempts() {
        return maxAttempts;
    }

    public double getJitterFraction() {
        return jitterFraction;
    }

    long applyJitter(long baseSeconds) {
        if (baseSeconds <= 0L || jitterFraction <= 0d) {
            return baseSeconds;
        }
        double maxDelta = baseSeconds * jitterFraction;
        double delta = (ThreadLocalRandom.current().nextDouble() * 2d - 1d) * maxDelta;
        long withJitter = Math.round(baseSeconds + delta);
        return Math.max(1L, withJitter);
    }
}
