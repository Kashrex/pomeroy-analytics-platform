CREATE TABLE IF NOT EXISTS BRONZE_TECHNICIAN_LANDING (
    run_id STRING NOT NULL, technician_id STRING, technician_name STRING, home_region STRING,
    active_flag BOOLEAN, source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
