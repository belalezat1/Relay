package com.relay.core.service;

import com.relay.core.model.Task;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * Enqueues task dispatch commands into the transactional outbox.
 * {@link OutboxPublisher} is the sole Kafka producer.
 */
@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTaskDispatchPublisher implements TaskDispatchPublisher {

    private final OutboxService outboxService;

    public KafkaTaskDispatchPublisher(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @Override
    public void publish(Task task) {
        if (task == null || task.getWorkflow() == null || task.getId() == null) {
            return;
        }

        TaskDispatchMessage message = TaskDispatchMessage.fromTask(task);
        outboxService.enqueueTaskDispatch(task.getWorkflow().getId(), message);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
