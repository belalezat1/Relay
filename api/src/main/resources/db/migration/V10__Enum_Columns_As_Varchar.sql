-- Hibernate @Enumerated(STRING) binds varchar; native PG enums require casts.

DROP INDEX IF EXISTS idx_tasks_status;
DROP INDEX IF EXISTS idx_tasks_claim_ready;

ALTER TABLE workflows ALTER COLUMN status DROP DEFAULT;
ALTER TABLE tasks ALTER COLUMN status DROP DEFAULT;

ALTER TABLE workflows
    ALTER COLUMN status TYPE VARCHAR(32) USING status::text;

ALTER TABLE tasks
    ALTER COLUMN status TYPE VARCHAR(32) USING status::text;

ALTER TABLE task_attempts
    ALTER COLUMN result TYPE VARCHAR(32) USING result::text;

ALTER TABLE workflows ALTER COLUMN status SET DEFAULT 'PENDING';
ALTER TABLE tasks ALTER COLUMN status SET DEFAULT 'PENDING';

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);

CREATE INDEX IF NOT EXISTS idx_tasks_claim_ready
    ON tasks (status, next_attempt_at)
    WHERE status IN ('PENDING', 'QUEUED');

DROP TYPE IF EXISTS workflow_status;
DROP TYPE IF EXISTS task_status;
DROP TYPE IF EXISTS task_result;
