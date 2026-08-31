package com.relay.core.service;

import com.relay.core.model.Task;
import com.relay.core.model.TaskStatus;
import com.relay.core.model.Workflow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DependencyGraphResolverTest {

    @Test
    void resolvesReadyTasksInDependencyOrder() {
        Workflow workflow = new Workflow();
        Task first = new Task();
        first.setId(UUID.randomUUID());
        first.setType("success");
        first.setStatus(TaskStatus.PENDING);
        first.setDependsOn(new UUID[0]);

        Task second = new Task();
        second.setId(UUID.randomUUID());
        second.setType("success");
        second.setStatus(TaskStatus.PENDING);
        second.setDependsOn(new UUID[] { first.getId() });

        Task third = new Task();
        third.setId(UUID.randomUUID());
        third.setType("success");
        third.setStatus(TaskStatus.SUCCEEDED);
        third.setDependsOn(new UUID[0]);

        workflow.setTasks(List.of(first, second, third));

        DependencyGraphResolver resolver = new DependencyGraphResolver();

        List<Task> readyTasks = resolver.getReadyTasks(workflow);

        assertThat(readyTasks).containsExactly(first);
        first.setStatus(TaskStatus.SUCCEEDED);
        assertThat(resolver.getReadyTasks(workflow)).containsExactly(second);
    }

    @Test
    void rejectsWorkflowCycles() {
        Workflow workflow = new Workflow();
        Task first = new Task();
        first.setId(UUID.randomUUID());
        first.setType("success");
        first.setStatus(TaskStatus.PENDING);
        first.setDependsOn(new UUID[0]);

        Task second = new Task();
        second.setId(UUID.randomUUID());
        second.setType("success");
        second.setStatus(TaskStatus.PENDING);
        second.setDependsOn(new UUID[] { first.getId() });

        first.setDependsOn(new UUID[] { second.getId() });
        workflow.setTasks(List.of(first, second));

        DependencyGraphResolver resolver = new DependencyGraphResolver();

        assertThatThrownBy(() -> resolver.getReadyTasks(workflow))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Cycle detected");
    }
}
