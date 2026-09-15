CREATE TABLE IF NOT EXISTS kafka_dispatch_failures (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic VARCHAR(255),
    payload TEXT NOT NULL,
    reason VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kafka_dispatch_failures_created
    ON kafka_dispatch_failures (created_at DESC);
