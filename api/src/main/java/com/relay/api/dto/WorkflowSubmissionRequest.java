package com.relay.api.dto;

import java.util.ArrayList;
import java.util.List;

public class WorkflowSubmissionRequest {

    private List<TaskRequest> tasks = new ArrayList<>();

    public List<TaskRequest> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskRequest> tasks) {
        this.tasks = tasks == null ? new ArrayList<>() : tasks;
    }
}
