ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS execution_claimed_at TIMESTAMP NULL,
    ADD COLUMN IF NOT EXISTS execution_completed_at TIMESTAMP NULL;

CREATE TABLE IF NOT EXISTS dead_letter_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL UNIQUE REFERENCES tasks(id),
    workflow_id UUID NOT NULL REFERENCES workflows(id),
    attempt_count INTEGER NOT NULL,
    error TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_tasks_workflow
    ON dead_letter_tasks (workflow_id, created_at DESC);
