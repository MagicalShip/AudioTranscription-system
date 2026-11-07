CREATE TABLE IF NOT EXISTS transcription_results (
    job_id TEXT NOT NULL,
    segment_number INTEGER NOT NULL,
    model_version TEXT NOT NULL,
    text TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    processing_seconds DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, segment_number, model_version)
);
