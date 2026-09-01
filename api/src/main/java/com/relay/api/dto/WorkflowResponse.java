package com.relay.api.dto;

import com.relay.core.model.WorkflowStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class WorkflowResponse {

    private UUID id;
    private WorkflowStatus status;
    private String owner;
    private String environment;
    private Integer version = 1;
    private Instant createdAt;
    private Instant updatedAt;
    private long durationMs;
    private int totalTasks;
    private int succeededTasks;
    private int failedTasks;
    private String lastError;
    private List<TaskResponse> tasks = new ArrayList<>();

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public WorkflowStatus getStatus() {
        return status;
    }

    public void setStatus(WorkflowStatus status) {
        this.status = status;
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

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public void setTotalTasks(int totalTasks) {
        this.totalTasks = totalTasks;
    }

    public int getSucceededTasks() {
        return succeededTasks;
    }

    public void setSucceededTasks(int succeededTasks) {
        this.succeededTasks = succeededTasks;
    }

    public int getFailedTasks() {
        return failedTasks;
    }

    public void setFailedTasks(int failedTasks) {
        this.failedTasks = failedTasks;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public List<TaskResponse> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskResponse> tasks) {
        this.tasks = tasks == null ? new ArrayList<>() : tasks;
    }
}
