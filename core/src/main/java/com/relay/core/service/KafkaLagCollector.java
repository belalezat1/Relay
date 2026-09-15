package com.relay.core.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Service
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaLagCollector {

    private static final Logger log = LoggerFactory.getLogger(KafkaLagCollector.class);

    private final KafkaRuntimeMetrics metrics;
    private final String bootstrapServers;
    private final String taskConsumerGroup;

    public KafkaLagCollector(
        KafkaRuntimeMetrics metrics,
        @Value("${relay.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
        @Value("${relay.kafka.task-consumer.group-id:relay-workflow-task-group}") String taskConsumerGroup
    ) {
        this.metrics = metrics;
        this.bootstrapServers = bootstrapServers;
        this.taskConsumerGroup = taskConsumerGroup;
    }

    @Scheduled(fixedDelayString = "${relay.kafka.lag-poll-delay:15000}")
    public void collect() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                .listConsumerGroupOffsets(taskConsumerGroup)
                .partitionsToOffsetAndMetadata()
                .get(3, TimeUnit.SECONDS);

            if (committed == null || committed.isEmpty()) {
                metrics.recordConsumerLag(0);
                return;
            }

            Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
            for (TopicPartition partition : committed.keySet()) {
                latestRequest.put(partition, OffsetSpec.latest());
            }
            ListOffsetsResult latestResult = adminClient.listOffsets(latestRequest);

            long lag = 0L;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                long latest = latestResult.partitionResult(entry.getKey()).get(3, TimeUnit.SECONDS).offset();
                long current = entry.getValue() == null ? 0L : entry.getValue().offset();
                lag += Math.max(0L, latest - current);
            }
            metrics.recordConsumerLag(lag);
        } catch (Exception ex) {
            log.debug("Unable to collect Kafka consumer lag: {}", ex.getMessage());
        }
    }
}
