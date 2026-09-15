package com.relay.core.service;

import com.relay.core.model.Task;

import java.time.Instant;
import java.util.UUID;

public class TaskDispatchMessage {
    private UUID workflowId;
    private UUID taskId;
    private String taskType;
    private String adapterType;
    private String owner;
    private String environment;
    private Integer version;
    private String payload;
    private String idempotencyKey;
    private UUID[] dependsOn;
    private Instant dispatchedAt;
    private Instant retryAfter;
    private Integer attemptNumber;

    public static TaskDispatchMessage fromTask(Task task) {
        TaskDispatchMessage message = new TaskDispatchMessage();
        if (task == null || task.getWorkflow() == null) {
            return message;
        }
        message.setWorkflowId(task.getWorkflow().getId());
        message.setTaskId(task.getId());
        message.setTaskType(task.getType());
        message.setAdapterType(task.getAdapterType());
        message.setOwner(task.getOwner());
        message.setEnvironment(task.getEnvironment());
        message.setVersion(task.getVersion());
        message.setPayload(task.getPayload());
        message.setIdempotencyKey(task.getIdempotencyKey());
        message.setDependsOn(task.getDependsOn() == null ? new UUID[0] : task.getDependsOn());
        message.setDispatchedAt(Instant.now());
        message.setAttemptNumber(task.getAttemptCount());
        return message;
    }

    public UUID getWorkflowId() { return workflowId; }
    public void setWorkflowId(UUID workflowId) { this.workflowId = workflowId; }
    public UUID getTaskId() { return taskId; }
    public void setTaskId(UUID taskId) { this.taskId = taskId; }
    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    public String getAdapterType() { return adapterType; }
    public void setAdapterType(String adapterType) { this.adapterType = adapterType; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }
    public String getEnvironment() { return environment; }
    public void setEnvironment(String environment) { this.environment = environment; }
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public UUID[] getDependsOn() { return dependsOn; }
    public void setDependsOn(UUID[] dependsOn) { this.dependsOn = dependsOn; }
    public Instant getDispatchedAt() { return dispatchedAt; }
    public void setDispatchedAt(Instant dispatchedAt) { this.dispatchedAt = dispatchedAt; }
    public Instant getRetryAfter() { return retryAfter; }
    public void setRetryAfter(Instant retryAfter) { this.retryAfter = retryAfter; }
    public Integer getAttemptNumber() { return attemptNumber; }
    public void setAttemptNumber(Integer attemptNumber) { this.attemptNumber = attemptNumber; }
}
