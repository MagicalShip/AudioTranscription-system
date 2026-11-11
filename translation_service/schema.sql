CREATE TABLE IF NOT EXISTS translation_results (
    job_id UUID NOT NULL,
    model_version TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    translation TEXT NOT NULL,
    processing_seconds DOUBLE PRECISION NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, model_version, prompt_version)
);
