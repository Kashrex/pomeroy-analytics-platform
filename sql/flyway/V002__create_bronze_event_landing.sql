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
