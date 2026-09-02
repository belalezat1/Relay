package com.relay.core.service;

import com.relay.core.model.Workflow;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaWorkflowEventPublisherTest {

    @Test
    void publishesWorkflowLifecycleEventToConfiguredTopic() {
        CapturingKafkaTemplate kafkaTemplate = new CapturingKafkaTemplate();

        KafkaWorkflowEventPublisher publisher = new KafkaWorkflowEventPublisher(kafkaTemplate);
        ReflectionTestUtils.setField(publisher, "topic", "relay.workflow.events");

        Workflow workflow = new Workflow();
        workflow.setId(UUID.randomUUID());
        UUID taskId = UUID.randomUUID();

        publisher.publish("workflow.created", workflow, taskId, "Workflow created", Map.of("owner", "platform"));

        assertThat(kafkaTemplate.topic).isEqualTo("relay.workflow.events");
        assertThat(kafkaTemplate.key).isEqualTo(workflow.getId().toString());
        assertThat(kafkaTemplate.payload).isInstanceOf(Map.class);
        assertThat(((Map<String, Object>) kafkaTemplate.payload).get("eventType")).isEqualTo("workflow.created");
        assertThat(((Map<String, Object>) kafkaTemplate.payload).get("workflowId")).isEqualTo(workflow.getId().toString());
    }

    private static class CapturingKafkaTemplate extends KafkaTemplate<String, Object> {
        private String topic;
        private String key;
        private Object payload;

        private CapturingKafkaTemplate() {
            super(new DefaultKafkaProducerFactory<>(Map.of()));
        }

        @Override
        public CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object data) {
            this.topic = topic;
            this.key = key;
            this.payload = data;
            return CompletableFuture.completedFuture(null);
        }
    }
}
