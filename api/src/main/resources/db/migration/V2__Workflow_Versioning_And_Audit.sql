ALTER TABLE workflows ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS workflow_audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    event_type VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_workflow_audit_events_workflow_id ON workflow_audit_events (workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_audit_events_task_id ON workflow_audit_events (task_id);
CREATE INDEX IF NOT EXISTS idx_workflow_audit_events_created_at ON workflow_audit_events (created_at);
