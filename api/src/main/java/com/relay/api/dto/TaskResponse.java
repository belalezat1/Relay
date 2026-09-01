package com.relay.api.dto;

import com.relay.core.model.TaskResult;
import com.relay.core.model.TaskStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TaskResponse {

    private UUID id;
    private String type;
    private String adapterType = "inline";
    private String owner;
    private String environment;
    private Integer version = 1;
    private TaskStatus status;
    private TaskResult result;
    private Map<String, Object> payload;
    private List<String> dependsOn = new ArrayList<>();
    private int attemptCount;
    private Instant createdAt;
    private Instant updatedAt;
    private long durationMs;
    private String errorMessage;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAdapterType() {
        return adapterType == null || adapterType.isBlank() ? "inline" : adapterType;
    }

    public void setAdapterType(String adapterType) {
        this.adapterType = adapterType == null || adapterType.isBlank() ? "inline" : adapterType;
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

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public TaskResult getResult() {
        return result;
    }

    public void setResult(TaskResult result) {
        this.result = result;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public List<String> getDependsOn() {
        return dependsOn;
    }

    public void setDependsOn(List<String> dependsOn) {
        this.dependsOn = dependsOn == null ? new ArrayList<>() : dependsOn;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
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

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
