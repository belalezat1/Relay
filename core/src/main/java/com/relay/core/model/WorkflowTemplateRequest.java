package com.relay.core.model;

import java.util.ArrayList;
import java.util.List;

public class WorkflowTemplateRequest {

    private String name;
    private String description;
    private String category = "general";
    private String owner;
    private String environment;
    private Integer timeoutSeconds = 0;
    private Integer slaThresholdSeconds = 0;
    private List<TaskDefinition> tasks = new ArrayList<>();

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

    public List<TaskDefinition> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDefinition> tasks) {
        this.tasks = tasks == null ? new ArrayList<>() : tasks;
    }
}
