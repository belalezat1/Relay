package com.relay.core.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Lob;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(
    name = "workflow_templates",
    uniqueConstraints = @UniqueConstraint(name = "uk_workflow_templates_name", columnNames = "name")
)
public class WorkflowTemplate {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", nullable = false, updatable = false, columnDefinition = "uuid")
    private UUID id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "category")
    private String category = "general";

    @Column(name = "owner")
    private String owner;

    @Column(name = "environment")
    private String environment;

    @Column(name = "timeout_seconds")
    private Integer timeoutSeconds = 0;

    @Column(name = "sla_threshold_seconds")
    private Integer slaThresholdSeconds = 0;

    @Column(name = "version", nullable = false)
    private Integer version = 1;

    @Lob
    @Column(name = "task_definitions", nullable = false)
    private String taskDefinitions = "[]";

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCategory() {
        return category == null || category.isBlank() ? "general" : category;
    }

    public void setCategory(String category) {
        this.category = category == null || category.isBlank() ? "general" : category;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public Integer getTimeoutSeconds() {
        return timeoutSeconds == null ? 0 : timeoutSeconds;
    }

    public void setTimeoutSeconds(Integer timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds == null ? 0 : timeoutSeconds;
    }

    public Integer getSlaThresholdSeconds() {
        return slaThresholdSeconds == null ? 0 : slaThresholdSeconds;
    }

    public void setSlaThresholdSeconds(Integer slaThresholdSeconds) {
        this.slaThresholdSeconds = slaThresholdSeconds == null ? 0 : slaThresholdSeconds;
    }

    public Integer getVersion() {
        return version == null ? 1 : version;
    }

    public void setVersion(Integer version) {
        this.version = version == null ? 1 : version;
    }

    public String getTaskDefinitions() {
        return taskDefinitions == null || taskDefinitions.isBlank() ? "[]" : taskDefinitions;
    }

    public void setTaskDefinitions(String taskDefinitions) {
        this.taskDefinitions = taskDefinitions == null || taskDefinitions.isBlank() ? "[]" : taskDefinitions;
    }

    public void setTaskDefinitions(List<TaskDefinition> taskDefinitions, ObjectMapper objectMapper) {
        if (taskDefinitions == null || taskDefinitions.isEmpty()) {
            this.taskDefinitions = "[]";
            return;
        }
        try {
            this.taskDefinitions = objectMapper.writeValueAsString(taskDefinitions);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize workflow template task definitions", e);
        }
    }

    public List<TaskDefinition> getTaskDefinitionsAsList(ObjectMapper objectMapper) {
        String value = getTaskDefinitions();
        if (value == null || value.isBlank() || "[]".equals(value.trim())) {
            return new ArrayList<>();
        }
        try {
            return objectMapper.readValue(value, new TypeReference<List<TaskDefinition>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to deserialize workflow template task definitions", e);
        }
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
