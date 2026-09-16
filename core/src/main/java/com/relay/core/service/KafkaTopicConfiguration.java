package com.relay.core.service;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Ensures task/retry/event topics exist with enough partitions for competing consumers.
 * Kafka keys remain workflow id (per-workflow ordering); partition count enables 1→N worker scale-out.
 */
@Configuration
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaTopicConfiguration {

    @Bean
    public NewTopic relayWorkflowTasksTopic(
        @Value("${relay.kafka.task-topic:relay.workflow.tasks}") String name,
        @Value("${relay.kafka.topic-partitions:12}") int partitions
    ) {
        return TopicBuilder.name(name).partitions(Math.max(3, partitions)).replicas(1).build();
    }

    @Bean
    public NewTopic relayWorkflowTaskRetryTopic(
        @Value("${relay.kafka.task-retry-topic:relay.workflow.tasks.retry}") String name,
        @Value("${relay.kafka.topic-partitions:12}") int partitions
    ) {
        return TopicBuilder.name(name).partitions(Math.max(3, partitions)).replicas(1).build();
    }

    @Bean
    public NewTopic relayWorkflowEventsTopic(
        @Value("${relay.kafka.topic:relay.workflow.events}") String name,
        @Value("${relay.kafka.topic-partitions:12}") int partitions
    ) {
        return TopicBuilder.name(name).partitions(Math.max(3, partitions)).replicas(1).build();
    }
}
