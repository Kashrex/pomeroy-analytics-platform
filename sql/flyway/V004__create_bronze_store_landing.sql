CREATE TABLE IF NOT EXISTS BRONZE_STORE_LANDING (
    run_id STRING NOT NULL, store_id STRING, client_id STRING, store_number STRING, region STRING,
    active_flag BOOLEAN, source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
