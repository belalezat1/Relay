package com.relay.core.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Consumes delayed task retries from the Kafka retry topic and re-enqueues them
 * onto the main task dispatch topic via the transactional outbox.
 * <p>
 * Delay is enforced by outbox {@code next_attempt_at} before publish to this topic;
 * if a message arrives early, it is re-queued with the remaining delay.
 */
@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class TaskRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskRetryConsumer.class);

    private final OutboxService outboxService;

    public TaskRetryConsumer(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @KafkaListener(
        topics = "${relay.kafka.task-retry-topic:relay.workflow.tasks.retry}",
        groupId = "${relay.kafka.task-retry-consumer.group-id:relay-workflow-task-retry-group}"
    )
    @Transactional
    public void consume(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty() || payload.get("taskId") == null) {
            log.warn("Ignoring invalid retry payload: {}", payload);
            return;
        }

        Instant retryAfter = parseInstant(payload.get("retryAfter"));
        UUID workflowId = parseUuid(payload.get("workflowId"));
        if (workflowId == null) {
            log.warn("Ignoring retry payload without workflowId: {}", payload);
            return;
        }

        if (retryAfter != null && retryAfter.isAfter(Instant.now())) {
            log.debug("Retry message for task {} not due until {}; re-queueing", payload.get("taskId"), retryAfter);
            outboxService.enqueueTaskRetry(workflowId, payload, retryAfter);
            return;
        }

        // Due: promote to main task topic through outbox (immediate).
        payload.remove("retryAfter");
        outboxService.enqueueTaskDispatch(workflowId, payload);
        log.info("Promoted retry for task {} onto main task dispatch topic", payload.get("taskId"));
    }

    private Instant parseInstant(Object raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Instant.parse(String.valueOf(raw));
        } catch (Exception ex) {
            return null;
        }
    }

    private UUID parseUuid(Object raw) {
        if (raw == null) {
            return null;
        }
        try {
            return UUID.fromString(String.valueOf(raw));
        } catch (Exception ex) {
            return null;
        }
    }
}
