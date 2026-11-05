CREATE TABLE IF NOT EXISTS audio_jobs (
    job_id TEXT PRIMARY KEY,
    source_object TEXT NOT NULL,
    status TEXT NOT NULL,
    duration_seconds DOUBLE PRECISION,
    total_segments INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audio_segments (
    job_id TEXT NOT NULL,
    segment_number INTEGER NOT NULL,
    object_name TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    audio_seconds DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (job_id, segment_number)
);
