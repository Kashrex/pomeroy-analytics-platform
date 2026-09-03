-- Depends on the Silver event table created by V006.
CREATE OR REPLACE PROCEDURE APPLY_SILVER_EVENT_CHANGES(P_RUN_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
  MERGE INTO SILVER_WORK_ORDER_EVENTS target
  USING (
    SELECT * EXCLUDE (rn) FROM (
      SELECT event_id, work_order_id, client_id, event_type, event_timestamp_utc, updated_at_utc,
             priority, technician_id, store_id, region, labor_minutes, source_system, payload_hash,
             source_file, source_row_number,
             ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at_utc DESC, ingested_at DESC, ingestion_id DESC) rn
      FROM BRONZE_EVENT_LANDING WHERE run_id = :P_RUN_ID
    ) WHERE rn = 1
  ) source ON target.event_id = source.event_id
  WHEN MATCHED AND (source.updated_at_utc > target.updated_at_utc
                    OR (source.updated_at_utc = target.updated_at_utc AND source.payload_hash <> target.payload_hash)) THEN
    UPDATE SET work_order_id=source.work_order_id, client_id=source.client_id, event_type=source.event_type,
      event_timestamp_utc=source.event_timestamp_utc, updated_at_utc=source.updated_at_utc,
      priority=source.priority, technician_id=source.technician_id, store_id=source.store_id, region=source.region,
      labor_minutes=source.labor_minutes, source_system=source.source_system, payload_hash=source.payload_hash,
      source_file=source.source_file, source_row_number=source.source_row_number, last_seen_at=CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (event_id,work_order_id,client_id,event_type,event_timestamp_utc,updated_at_utc,
    priority,technician_id,store_id,region,labor_minutes,source_system,payload_hash,source_file,source_row_number)
  VALUES (source.event_id,source.work_order_id,source.client_id,source.event_type,source.event_timestamp_utc,source.updated_at_utc,
    source.priority,source.technician_id,source.store_id,source.region,source.labor_minutes,source.source_system,
    source.payload_hash,source.source_file,source.source_row_number);
  RETURN 'SILVER event merge complete';
END;
$$;
