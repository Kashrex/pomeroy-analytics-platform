CREATE TABLE IF NOT EXISTS INGESTION_RUNS (
    run_id STRING PRIMARY KEY,
    source_checksum STRING NOT NULL,
    started_at TIMESTAMP_TZ NOT NULL,
    completed_at TIMESTAMP_TZ,
    valid_event_count NUMBER DEFAULT 0,
    rejected_event_count NUMBER DEFAULT 0,
    status STRING NOT NULL,
    error_message STRING
);
