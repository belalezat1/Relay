package com.relay.api.health;

import com.relay.core.service.KafkaRuntimeMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaRuntimeHealthIndicator implements HealthIndicator {

    private final String bootstrapServers;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final KafkaRuntimeMetrics metrics;

    public KafkaRuntimeHealthIndicator(
        @Value("${relay.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
        ObjectProvider<KafkaListenerEndpointRegistry> listenerRegistryProvider,
        ObjectProvider<KafkaRuntimeMetrics> metricsProvider
    ) {
        this.bootstrapServers = bootstrapServers;
        this.listenerRegistry = listenerRegistryProvider.getIfAvailable();
        this.metrics = metricsProvider.getIfAvailable();
    }

    @Override
    public Health health() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            DescribeClusterResult cluster = adminClient.describeCluster();
            int nodeCount = cluster.nodes().get(3, TimeUnit.SECONDS).size();
            boolean listenersRunning = listenerRegistry == null
                || listenerRegistry.getListenerContainers().stream().allMatch(MessageListenerContainer::isRunning);

            Health.Builder builder = listenersRunning && nodeCount > 0 ? Health.up() : Health.down();
            builder.withDetail("bootstrapServers", bootstrapServers);
            builder.withDetail("brokerNodes", nodeCount);
            builder.withDetail("listenersRunning", listenersRunning);
            if (metrics != null) {
                builder.withDetail("taskConsumerLag", metrics.consumerLag());
            }
            return builder.build();
        } catch (Exception ex) {
            return Health.down(ex)
                .withDetail("bootstrapServers", bootstrapServers)
                .withDetail("error", ex.getMessage())
                .build();
        }
    }
}
