package com.relay.core.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.OutboxEvent;
import com.relay.core.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Polls ready outbox rows and publishes them to Kafka. Transient broker failures
 * reschedule the row with exponential backoff + jitter instead of sticky FAILED.
 * Permanent FAILED is only set after {@code relay.outbox.max-attempts}.
 */
@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaRuntimeMetrics metrics;
    private final String workflowTopic;
    private final String taskTopic;
    private final String taskRetryTopic;
    private final int batchSize;
    private final boolean skipLocked;
    private final int maxAttempts;
    private final long initialBackoffSeconds;
    private final long backoffMultiplier;
    private final double jitterFraction;

    public OutboxPublisher(
        OutboxEventRepository outboxEventRepository,
        KafkaTemplate<String, Object> kafkaTemplate,
        ObjectMapper objectMapper,
        @Autowired(required = false) KafkaRuntimeMetrics metrics,
        @Value("${relay.kafka.topic:relay.workflow.events}") String workflowTopic,
        @Value("${relay.kafka.task-topic:relay.workflow.tasks}") String taskTopic,
        @Value("${relay.kafka.task-retry-topic:relay.workflow.tasks.retry}") String taskRetryTopic,
        @Value("${relay.outbox.batch-size:50}") int batchSize,
        @Value("${relay.task.claim.skip-locked:true}") boolean skipLocked,
        @Value("${relay.outbox.max-attempts:8}") int maxAttempts,
        @Value("${relay.outbox.initial-backoff-seconds:2}") long initialBackoffSeconds,
        @Value("${relay.outbox.backoff-multiplier:2}") long backoffMultiplier,
        @Value("${relay.outbox.jitter-fraction:0.2}") double jitterFraction
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.workflowTopic = workflowTopic;
        this.taskTopic = taskTopic;
        this.taskRetryTopic = taskRetryTopic;
        this.batchSize = Math.max(1, batchSize);
        this.skipLocked = skipLocked;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoffSeconds = Math.max(1L, initialBackoffSeconds);
        this.backoffMultiplier = Math.max(1L, backoffMultiplier);
        this.jitterFraction = Math.max(0d, Math.min(1d, jitterFraction));
    }

    @Scheduled(fixedDelayString = "${relay.outbox.poll-delay:100}")
    @Transactional
    public int publishPending() {
        Instant now = Instant.now();
        List<OutboxEvent> pending = loadReady(now);
        if (pending.isEmpty()) {
            return 0;
        }

        List<PendingSend> sends = new ArrayList<>(pending.size());
        for (OutboxEvent event : pending) {
            try {
                Object payload = deserialize(event.getPayload());
                String topic = resolveTopic(event);
                String key = event.getAggregateId() == null ? event.getId().toString() : event.getAggregateId().toString();
                sends.add(new PendingSend(event, kafkaTemplate.send(topic, key, payload)));
            } catch (Exception ex) {
                scheduleRetry(event, ex);
            }
        }

        List<OutboxEvent> publishedEvents = new ArrayList<>(sends.size());
        Instant publishedAt = Instant.now();
        for (PendingSend send : sends) {
            try {
                send.future.get(10, TimeUnit.SECONDS);
                send.event.setStatus(OutboxEvent.STATUS_PUBLISHED);
                send.event.setPublishedAt(publishedAt);
                send.event.setLastError(null);
                send.event.setNextAttemptAt(null);
                publishedEvents.add(send.event);
                if (metrics != null && OutboxEvent.EVENT_TASK_DISPATCH.equals(send.event.getEventType())) {
                    metrics.taskDispatched();
                }
            } catch (Exception ex) {
                scheduleRetry(send.event, unwrap(ex));
            }
        }
        if (!publishedEvents.isEmpty()) {
            outboxEventRepository.saveAll(publishedEvents);
        }
        return publishedEvents.size();
    }

    private Exception unwrap(Exception ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof Exception nested) {
            return nested;
        }
        return ex;
    }

    private record PendingSend(OutboxEvent event, CompletableFuture<SendResult<String, Object>> future) {
    }

    private void scheduleRetry(OutboxEvent event, Exception ex) {
        int nextAttempt = event.getAttemptCount() + 1;
        event.setAttemptCount(nextAttempt);
        event.setLastError(ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());

        if (nextAttempt >= maxAttempts) {
            log.error("Outbox event {} exhausted {} publish attempts; marking FAILED", event.getId(), maxAttempts, ex);
            event.setStatus(OutboxEvent.STATUS_FAILED);
            event.setNextAttemptAt(null);
            outboxEventRepository.save(event);
            if (metrics != null && OutboxEvent.EVENT_TASK_DISPATCH.equals(event.getEventType())) {
                metrics.taskPublishFailed();
            }
            return;
        }

        long delaySeconds = computeBackoffSeconds(nextAttempt);
        event.setStatus(OutboxEvent.STATUS_PENDING);
        event.setNextAttemptAt(Instant.now().plusSeconds(delaySeconds));
        outboxEventRepository.save(event);
        log.warn(
            "Outbox event {} publish failed (attempt {}/ {}); retrying in {}s: {}",
            event.getId(),
            nextAttempt,
            maxAttempts,
            delaySeconds,
            ex.getMessage()
        );
        if (metrics != null && OutboxEvent.EVENT_TASK_DISPATCH.equals(event.getEventType())) {
            metrics.taskPublishFailed();
        }
    }

    private long computeBackoffSeconds(int attemptNumber) {
        long exp = initialBackoffSeconds * (long) Math.pow(backoffMultiplier, Math.max(0, attemptNumber - 1));
        if (jitterFraction <= 0d) {
            return Math.max(1L, exp);
        }
        double delta = exp * jitterFraction;
        long jittered = Math.round(exp + (ThreadLocalRandom.current().nextDouble() * 2 * delta - delta));
        return Math.max(1L, jittered);
    }

    private List<OutboxEvent> loadReady(Instant now) {
        if (skipLocked) {
            try {
                List<OutboxEvent> locked = outboxEventRepository.findReadyForUpdateSkipLocked(now, batchSize);
                if (!locked.isEmpty()) {
                    return locked;
                }
            } catch (RuntimeException ex) {
                log.debug("Outbox SKIP LOCKED unavailable; using pessimistic lock fallback: {}", ex.getMessage());
            }
        }
        List<OutboxEvent> all = outboxEventRepository.findReadyForUpdate(now);
        return all.size() <= batchSize ? all : all.subList(0, batchSize);
    }

    private String resolveTopic(OutboxEvent event) {
        if (OutboxEvent.EVENT_TASK_RETRY.equals(event.getEventType())) {
            return taskRetryTopic;
        }
        if (OutboxEvent.EVENT_TASK_DISPATCH.equals(event.getEventType())) {
            return taskTopic;
        }
        return workflowTopic;
    }

    private Object deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, new TypeReference<Map<String, Object>>() {});
        } catch (Exception ex) {
            return payload;
        }
    }
}
