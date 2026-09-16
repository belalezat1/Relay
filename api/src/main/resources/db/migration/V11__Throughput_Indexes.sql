-- Hot-path indexes for orchestrator polling and outbox drain under multi-worker load.

CREATE INDEX IF NOT EXISTS idx_workflows_status_created
    ON workflows (status, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_events_ready
    ON outbox_events (status, next_attempt_at, created_at)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_tasks_status_updated
    ON tasks (status, updated_at);
