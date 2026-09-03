CREATE TABLE IF NOT EXISTS SILVER_WORK_ORDER_EVENTS (
    event_id STRING PRIMARY KEY,
    work_order_id STRING NOT NULL, client_id STRING NOT NULL, event_type STRING NOT NULL,
    event_timestamp_utc TIMESTAMP_TZ NOT NULL, updated_at_utc TIMESTAMP_TZ NOT NULL,
    priority STRING, technician_id STRING, store_id STRING, region STRING,
    labor_minutes NUMBER(10,0), source_system STRING, payload_hash STRING NOT NULL,
    source_file STRING NOT NULL, source_row_number NUMBER NOT NULL,
    first_seen_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    last_seen_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);
