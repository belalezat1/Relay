package com.relay.core.service;

import com.relay.core.model.Task;

public interface TaskDispatchPublisher {
    void publish(Task task);

    default boolean isEnabled() {
        return false;
    }
}
