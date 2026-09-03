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

CREATE TABLE IF NOT EXISTS BRONZE_EVENT_LANDING (
    ingestion_id NUMBER AUTOINCREMENT,
    run_id STRING NOT NULL,
    event_id STRING, work_order_id STRING, client_id STRING, event_type STRING,
    event_timestamp_utc TIMESTAMP_TZ, updated_at_utc TIMESTAMP_TZ,
    priority STRING, technician_id STRING, store_id STRING, region STRING,
    labor_minutes NUMBER(10,0), source_system STRING,
    source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    payload_hash STRING NOT NULL, raw_payload VARIANT NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE_REJECTED_EVENTS (
    rejection_id NUMBER AUTOINCREMENT,
    run_id STRING NOT NULL, source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    rejection_reason STRING NOT NULL, raw_payload VARIANT,
    rejected_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE_STORE_LANDING (
    run_id STRING NOT NULL, store_id STRING, client_id STRING, store_number STRING, region STRING,
    active_flag BOOLEAN, source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE_TECHNICIAN_LANDING (
    run_id STRING NOT NULL, technician_id STRING, technician_name STRING, home_region STRING,
    active_flag BOOLEAN, source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
