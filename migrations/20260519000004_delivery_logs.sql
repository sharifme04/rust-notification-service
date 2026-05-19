CREATE TABLE delivery_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_job_id UUID NOT NULL REFERENCES delivery_jobs(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    status delivery_status NOT NULL,
    response_code INT,
    error_detail TEXT,
    duration_ms INT,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_delivery_logs_job_id ON delivery_logs(delivery_job_id);
