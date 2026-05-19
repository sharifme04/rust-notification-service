CREATE TYPE delivery_channel AS ENUM ('email', 'webhook', 'in_app');
CREATE TYPE delivery_status AS ENUM ('queued', 'delivered', 'failed', 'retrying');

CREATE TABLE delivery_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id UUID NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    channel delivery_channel NOT NULL,
    status delivery_status NOT NULL DEFAULT 'queued',
    attempt_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    next_attempt_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_delivery_jobs_notification_id ON delivery_jobs(notification_id);
CREATE INDEX idx_delivery_jobs_status ON delivery_jobs(status);
CREATE INDEX idx_delivery_jobs_next_attempt ON delivery_jobs(next_attempt_at)
    WHERE status IN ('queued', 'retrying');
