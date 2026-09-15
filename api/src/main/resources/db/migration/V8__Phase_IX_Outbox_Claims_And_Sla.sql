-- Phase IX: claim columns, SLA fields, transactional outbox, projections, idempotency outcomes

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP NULL;

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS locked_by VARCHAR(255) NULL;

ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP NULL;

ALTER TABLE workflows
    ADD COLUMN IF NOT EXISTS timeout_seconds INTEGER NULL;

ALTER TABLE workflows
    ADD COLUMN IF NOT EXISTS sla_threshold_seconds INTEGER NULL;

ALTER TABLE dead_letter_tasks
    ADD COLUMN IF NOT EXISTS replayed_at TIMESTAMP NULL;

-- Allow historical DLQ rows after replay while keeping one active DLQ per task
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dead_letter_tasks_task_id_key'
    ) THEN
        ALTER TABLE dead_letter_tasks DROP CONSTRAINT dead_letter_tasks_task_id_key;
    END IF;
EXCEPTION
    WHEN undefined_object THEN
        NULL;
END $$;

DROP INDEX IF EXISTS dead_letter_tasks_task_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_dead_letter_tasks_active_task
    ON dead_letter_tasks (task_id)
    WHERE replayed_at IS NULL;

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING'
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished
    ON outbox_events (status, created_at)
    WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS workflow_event_projections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(128) NOT NULL,
    workflow_id UUID NOT NULL,
    task_id UUID NULL,
    message TEXT,
    metadata TEXT,
    received_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_event_projections_workflow
    ON workflow_event_projections (workflow_id, received_at DESC);

CREATE TABLE IF NOT EXISTS idempotency_outcomes (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id),
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    status VARCHAR(32) NOT NULL,
    result VARCHAR(32),
    completed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim_ready
    ON tasks (status, next_attempt_at)
    WHERE status IN ('PENDING', 'QUEUED');

CREATE INDEX IF NOT EXISTS idx_tasks_locked_by
    ON tasks (locked_by)
    WHERE locked_by IS NOT NULL;
