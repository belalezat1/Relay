package com.relay.api.controller;

import com.relay.core.model.DeadLetterTask;
import com.relay.core.model.Task;
import com.relay.core.service.DeadLetterTaskService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/dead-letters")
public class DeadLetterController {

    private final DeadLetterTaskService deadLetterTaskService;

    public DeadLetterController(DeadLetterTaskService deadLetterTaskService) {
        this.deadLetterTaskService = deadLetterTaskService;
    }

    @GetMapping
    public ResponseEntity<List<Map<String, Object>>> list(@RequestParam(name = "workflowId", required = false) UUID workflowId) {
        List<Map<String, Object>> response = deadLetterTaskService.list(workflowId).stream()
            .map(this::toDeadLetterResponse)
            .collect(Collectors.toList());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable("id") UUID id) {
        try {
            Task replayed = deadLetterTaskService.replay(id);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("taskId", replayed.getId());
            response.put("workflowId", replayed.getWorkflow() == null ? null : replayed.getWorkflow().getId());
            response.put("taskStatus", replayed.getStatus() == null ? null : replayed.getStatus().name());
            response.put("attemptCount", replayed.getAttemptCount());
            response.put("intervention", "replayed");
            response.put("note", "Task reset to PENDING; side effects re-run when rediscovered");
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage());
        } catch (IllegalStateException ex) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getMessage());
        }
    }

    private Map<String, Object> toDeadLetterResponse(DeadLetterTask deadLetter) {
        Task task = deadLetter.getTask();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", deadLetter.getId());
        response.put("taskId", task.getId());
        response.put("workflowId", deadLetter.getWorkflowId());
        response.put("taskType", task.getType());
        response.put("adapterType", task.getAdapterType());
        response.put("owner", task.getOwner());
        response.put("environment", task.getEnvironment());
        response.put("taskStatus", task.getStatus() == null ? null : task.getStatus().name());
        response.put("idempotencyKey", task.getIdempotencyKey());
        response.put("attemptCount", deadLetter.getAttemptCount());
        response.put("error", deadLetter.getError());
        response.put("createdAt", deadLetter.getCreatedAt());
        response.put("intervention", "manual");
        return response;
    }
}
