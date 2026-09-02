package com.relay.core.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.relay.core.model.Task;
import com.relay.core.model.TaskResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskExecutionRegistryTest {

    @Test
    void exposesAdapterMetadataAndAliases() {
        TaskExecutionRegistry registry = new TaskExecutionRegistry(new ObjectMapper());

        assertThat(registry.supportsAdapterType("http")).isTrue();
        assertThat(registry.supportsAdapterType("db")).isTrue();
        assertThat(registry.supportsAdapterType("database")).isTrue();
        assertThat(registry.supportsAdapterType("notification")).isTrue();

        assertThat(registry.listAdapterCapabilities())
            .extracting(capability -> capability.get("type"))
            .contains("db", "http", "notification");

        assertThat(registry.listAdapterCapabilities())
            .anySatisfy(capability -> {
                assertThat(capability.get("type")).isEqualTo("notification");
                assertThat(capability.get("supportsAsync")).isEqualTo(true);
                assertThat(capability.get("capabilities")).asString().contains("notify");
            });
    }

    @Test
    void resolvesNotificationTasks() {
        TaskExecutionRegistry registry = new TaskExecutionRegistry(new ObjectMapper());
        Task task = new Task();
        task.setAdapterType("notification");
        task.setType("notify_user");
        task.setPayload("{\"message\":\"hello\"}");

        assertThat(registry.resolve(task).execute(task)).isEqualTo(TaskResult.SUCCESS);
    }
}
