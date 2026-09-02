package com.relay.core.service;

import com.relay.core.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true")
public class KafkaTaskDispatchPublisher implements TaskDispatchPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTaskDispatchPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${relay.kafka.task-topic:relay.workflow.tasks}")
    private String topic;

    public KafkaTaskDispatchPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(Task task) {
        if (task == null || task.getWorkflow() == null || task.getId() == null) {
            return;
        }

        TaskDispatchMessage message = TaskDispatchMessage.fromTask(task);
        kafkaTemplate.send(topic, task.getWorkflow().getId().toString(), message)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.warn("Failed to publish task dispatch for task {} to Kafka topic {}", task.getId(), topic, ex);
                }
            });
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
