package com.relay.api.dto;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class WorkflowTemplateResponse {

    private UUID id;
    private String name;
    private String description;
    private String category = "general";
    private String owner;
    private String environment;
    private Integer timeoutSeconds = 0;
    private Integer slaThresholdSeconds = 0;
    private Integer version = 1;
    private Instant createdAt;
    private Instant updatedAt;
    private List<TaskDefinitionResponse> tasks = new ArrayList<>();

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

    public List<TaskDefinitionResponse> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDefinitionResponse> tasks) {
        this.tasks = tasks == null ? new ArrayList<>() : tasks;
    }
}
