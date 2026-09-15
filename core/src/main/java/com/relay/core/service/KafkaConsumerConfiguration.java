package com.relay.core.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@ConditionalOnProperty(name = "relay.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConsumerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfiguration.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
        KafkaDispatchFailureService dispatchFailureService,
        org.springframework.beans.factory.ObjectProvider<KafkaRuntimeMetrics> metricsProvider
    ) {
        KafkaRuntimeMetrics metrics = metricsProvider.getIfAvailable();
        return new DefaultErrorHandler((ConsumerRecord<?, ?> record, Exception exception) -> {
            log.error("Kafka listener exhausted retries for topic {} partition {} offset {}",
                record.topic(), record.partition(), record.offset(), exception);
            if (metrics != null) {
                metrics.taskFailed();
            }
            String payload = record.value() == null ? "{}" : String.valueOf(record.value());
            dispatchFailureService.record(record.topic(), payload, "listener-exhausted");
        }, new FixedBackOff(1000L, 3L));
    }
}
