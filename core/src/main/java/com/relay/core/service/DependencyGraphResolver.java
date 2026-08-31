package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
public class DependencyGraphResolver {

    public List<Task> getReadyTasks(Workflow workflow) {
        Map<UUID, Task> tasksById = new HashMap<>();
        for (Task task : workflow.getTasks()) {
            tasksById.put(task.getId(), task);
        }

        validateNoCycles(workflow.getTasks(), tasksById);

        List<Task> readyTasks = new ArrayList<>();
        for (Task task : workflow.getTasks()) {
            if (task.getStatus() != TaskStatus.PENDING) {
                continue;
            }

            boolean ready = true;
            for (UUID dependencyId : task.getDependsOn()) {
                Task dependency = tasksById.get(dependencyId);
                if (dependency == null) {
                    throw new IllegalArgumentException(
                        "Task " + task.getId() + " references missing dependency " + dependencyId
                    );
                }
                if (dependency.getStatus() != TaskStatus.SUCCEEDED) {
                    ready = false;
                    break;
                }
            }

            if (ready) {
                readyTasks.add(task);
            }
        }

        return readyTasks;
    }

    private void validateNoCycles(List<Task> tasks, Map<UUID, Task> tasksById) {
        Set<UUID> visiting = new HashSet<>();
        Set<UUID> visited = new HashSet<>();

        for (Task task : tasks) {
            dfs(task, tasksById, visiting, visited);
        }
    }

    private void dfs(Task task, Map<UUID, Task> tasksById, Set<UUID> visiting, Set<UUID> visited) {
        if (task == null) {
            return;
        }

        UUID taskId = task.getId();
        if (visiting.contains(taskId)) {
            throw new IllegalStateException("Cycle detected in workflow dependency graph at task: " + taskId);
        }
        if (visited.contains(taskId)) {
            return;
        }

        visiting.add(taskId);
        for (UUID dependencyId : task.getDependsOn()) {
            Task dependency = tasksById.get(dependencyId);
            if (dependency == null) {
                continue;
            }
            dfs(dependency, tasksById, visiting, visited);
        }
        visiting.remove(taskId);
        visited.add(taskId);
    }
}
