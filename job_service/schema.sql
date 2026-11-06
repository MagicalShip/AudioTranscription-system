CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    source_language TEXT NOT NULL CHECK (source_language = 'en'),
    target_language TEXT NOT NULL CHECK (target_language = 'zh'),
    object_name TEXT NOT NULL,
    status TEXT NOT NULL,
    total_segments INTEGER,
    completed_segments INTEGER NOT NULL DEFAULT 0,
    failed_segments INTEGER NOT NULL DEFAULT 0,
    transcript TEXT,
    translation TEXT,
    error TEXT,
    model_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS transcript_segments (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    segment_number INTEGER NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    status TEXT NOT NULL,
    text TEXT,
    error TEXT,
    model_version TEXT,
    processing_seconds DOUBLE PRECISION,
    PRIMARY KEY (job_id, segment_number)
);

CREATE TABLE IF NOT EXISTS job_inbox (
    event_id TEXT PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_outbox (
    id BIGSERIAL PRIMARY KEY,
    routing_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_job_outbox_unpublished ON job_outbox(id) WHERE published_at IS NULL;
