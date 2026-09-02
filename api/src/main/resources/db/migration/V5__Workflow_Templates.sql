CREATE TABLE IF NOT EXISTS workflow_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(128) NOT NULL DEFAULT 'general',
    owner VARCHAR(255),
    environment VARCHAR(64),
    timeout_seconds INTEGER NOT NULL DEFAULT 0,
    sla_threshold_seconds INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    task_definitions JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uk_workflow_templates_name
    ON workflow_templates (name);

CREATE INDEX IF NOT EXISTS idx_workflow_templates_category
    ON workflow_templates (category);
