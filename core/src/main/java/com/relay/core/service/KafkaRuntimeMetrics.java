package com.relay.core.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaRuntimeMetrics {

    private final Counter dispatchedTasks;
    private final Counter consumedTasks;
    private final Counter failedTasks;
    private final Counter deadLetteredTasks;
    private final Counter duplicateTasks;
    private final Counter retriedTasks;
    private final Counter invalidTasks;
    private final Counter publishFailures;
    private final Timer taskProcessingTime;
    private final AtomicLong taskConsumerLag = new AtomicLong(0);

    public KafkaRuntimeMetrics(MeterRegistry registry) {
        this.dispatchedTasks = registry.counter("relay.kafka.tasks.dispatched");
        this.consumedTasks = registry.counter("relay.kafka.tasks.consumed");
        this.failedTasks = registry.counter("relay.kafka.tasks.failed");
        this.deadLetteredTasks = registry.counter("relay.kafka.tasks.dead_lettered");
        this.duplicateTasks = registry.counter("relay.kafka.tasks.duplicate");
        this.retriedTasks = registry.counter("relay.kafka.tasks.retried");
        this.invalidTasks = registry.counter("relay.kafka.tasks.invalid");
        this.publishFailures = registry.counter("relay.kafka.tasks.publish_failed");
        this.taskProcessingTime = registry.timer("relay.kafka.tasks.processing");
        registry.gauge("relay.kafka.consumer.lag", taskConsumerLag);
    }

    public void taskDispatched() {
        dispatchedTasks.increment();
    }

    public void taskConsumed() {
        consumedTasks.increment();
    }

    public void taskFailed() {
        failedTasks.increment();
    }

    public void taskDeadLettered() {
        deadLetteredTasks.increment();
    }

    public void taskDuplicated() {
        duplicateTasks.increment();
    }

    public void taskRetried() {
        retriedTasks.increment();
    }

    public void taskInvalid() {
        invalidTasks.increment();
    }

    public void taskPublishFailed() {
        publishFailures.increment();
    }

    public void recordConsumerLag(long lag) {
        taskConsumerLag.set(Math.max(0L, lag));
    }

    public long consumerLag() {
        return taskConsumerLag.get();
    }

    public <T> T time(Callable<T> action) throws Exception {
        return taskProcessingTime.recordCallable(action);
    }
}
