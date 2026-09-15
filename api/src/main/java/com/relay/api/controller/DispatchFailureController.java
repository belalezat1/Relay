package com.relay.api.controller;

import com.relay.core.model.KafkaDispatchFailure;
import com.relay.core.service.KafkaDispatchFailureService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/dispatch-failures")
public class DispatchFailureController {

    private final KafkaDispatchFailureService kafkaDispatchFailureService;

    public DispatchFailureController(KafkaDispatchFailureService kafkaDispatchFailureService) {
        this.kafkaDispatchFailureService = kafkaDispatchFailureService;
    }

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> list() {
        List<Map<String, Object>> response = kafkaDispatchFailureService.list().stream()
            .map(this::toResponse)
            .collect(Collectors.toList());
        return ResponseEntity.ok(response);
    }

    private Map<String, Object> toResponse(KafkaDispatchFailure failure) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", failure.getId());
        response.put("topic", failure.getTopic());
        response.put("payload", failure.getPayload());
        response.put("reason", failure.getReason());
        response.put("createdAt", failure.getCreatedAt());
        response.put("intervention", "inspect-and-replay-source-workflow");
        return response;
    }
}
