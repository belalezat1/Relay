package com.relay.core.service;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowDispatchQueueTest {

    @Test
    void rejectsDuplicateWorkflowEntries() {
        WorkflowDispatchQueue queue = new WorkflowDispatchQueue();
        UUID workflowId = UUID.randomUUID();

        assertThat(queue.tryQueue(workflowId)).isTrue();
        assertThat(queue.tryQueue(workflowId)).isFalse();
        assertThat(queue.contains(workflowId)).isTrue();
        assertThat(queue.pollNext()).isEqualTo(workflowId);
        assertThat(queue.contains(workflowId)).isFalse();
    }
}
