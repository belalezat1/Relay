package com.relay.core.service;

import com.relay.core.model.Task;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "false")
public class NoOpTaskDispatchPublisher implements TaskDispatchPublisher {

    @Override
    public void publish(Task task) {
        // Intentionally no-op when Kafka-based task dispatch is disabled.
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
