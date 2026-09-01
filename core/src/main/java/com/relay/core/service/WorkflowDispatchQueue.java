package com.relay.core.service;

import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class WorkflowDispatchQueue {

    private final Queue<UUID> queue = new ConcurrentLinkedQueue<>();
    private final Set<UUID> queuedIds = ConcurrentHashMap.newKeySet();

    public boolean tryQueue(UUID workflowId) {
        if (workflowId == null || !queuedIds.add(workflowId)) {
            return false;
        }
        queue.add(workflowId);
        return true;
    }

    public UUID pollNext() {
        UUID workflowId = queue.poll();
        if (workflowId != null) {
            queuedIds.remove(workflowId);
        }
        return workflowId;
    }

    public boolean contains(UUID workflowId) {
        return workflowId != null && queuedIds.contains(workflowId);
    }

    public void complete(UUID workflowId) {
        if (workflowId != null) {
            queuedIds.remove(workflowId);
        }
    }
}
