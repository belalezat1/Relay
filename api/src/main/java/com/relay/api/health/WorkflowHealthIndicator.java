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
            return Health.up()
                .withDetail("workflowCount", workflowCount)
                .withDetail("persistence", "postgresql")
                .build();
        } catch (Exception ex) {
            return Health.down(ex)
                .withDetail("error", ex.getMessage())
                .build();
        }
    }
}
