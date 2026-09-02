package com.relay.core.service;

import com.relay.core.model.Task;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnMissingBean(TaskDispatchPublisher.class)
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
