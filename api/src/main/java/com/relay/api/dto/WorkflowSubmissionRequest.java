package com.relay.api.dto;

import java.util.ArrayList;
import java.util.List;

public class WorkflowSubmissionRequest {

    private String owner;
    private String environment;
    private List<TaskRequest> tasks = new ArrayList<>();

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

    public List<TaskRequest> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskRequest> tasks) {
        this.tasks = tasks == null ? new ArrayList<>() : tasks;
    }
}
