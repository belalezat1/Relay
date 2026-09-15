package com.relay.core.service;

import com.relay.core.model.KafkaDispatchFailure;
import com.relay.core.repository.KafkaDispatchFailureRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class KafkaDispatchFailureService {

    private static final Logger log = LoggerFactory.getLogger(KafkaDispatchFailureService.class);

    private final KafkaDispatchFailureRepository repository;

    public KafkaDispatchFailureService(KafkaDispatchFailureRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public KafkaDispatchFailure record(String topic, String payload, String reason) {
        KafkaDispatchFailure failure = new KafkaDispatchFailure();
        failure.setTopic(topic);
        failure.setPayload(payload == null || payload.isBlank() ? "{}" : truncate(payload, 8000));
        failure.setReason(reason == null || reason.isBlank() ? "unknown" : reason);
        log.error("Kafka dispatch failure [{}]: {}", failure.getReason(), failure.getPayload());
        return repository.save(failure);
    }

    @Transactional(readOnly = true)
    public List<KafkaDispatchFailure> list() {
        return repository.findAllByOrderByCreatedAtDesc();
    }

    private String truncate(String value, int maxLength) {
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }
}
