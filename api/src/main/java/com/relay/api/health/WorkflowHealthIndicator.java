package com.relay.api.health;

import com.relay.core.repository.WorkflowRepository;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class WorkflowHealthIndicator implements HealthIndicator {

    private final WorkflowRepository workflowRepository;

    public WorkflowHealthIndicator(WorkflowRepository workflowRepository) {
        this.workflowRepository = workflowRepository;
    }

    @Override
    public Health health() {
        try {
            long workflowCount = workflowRepository.count();
            var totals = workflowRepository.findAll().stream()
                .collect(java.util.stream.Collectors.groupingBy(com.relay.core.model.Workflow::getStatus, java.util.stream.Collectors.counting()));

            return Health.up()
                .withDetail("workflowCount", workflowCount)
                .withDetail("statusCounts", totals)
                .withDetail("persistence", "postgresql")
                .build();
        } catch (Exception ex) {
            return Health.down(ex)
                .withDetail("error", ex.getMessage())
                .build();
        }
    }
}
