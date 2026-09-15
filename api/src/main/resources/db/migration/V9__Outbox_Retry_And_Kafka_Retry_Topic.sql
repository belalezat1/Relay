-- Phase IX hardening: outbox publish retries + delayed readiness index

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP NULL;

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS last_error TEXT NULL;

DROP INDEX IF EXISTS idx_outbox_events_unpublished;

CREATE INDEX IF NOT EXISTS idx_outbox_events_ready
    ON outbox_events (status, next_attempt_at, created_at)
    WHERE status = 'PENDING';
