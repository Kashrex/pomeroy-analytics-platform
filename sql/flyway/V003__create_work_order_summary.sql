CREATE OR REPLACE VIEW WORK_ORDER_SUMMARY AS
WITH per_work_order AS (
  SELECT
    work_order_id,
    MAX_BY(client_id, updated_at_utc) AS client_id,
    MAX_BY(store_id, updated_at_utc) AS store_id,
    MAX_BY(technician_id, updated_at_utc) AS technician_id,
    MAX_BY(priority, updated_at_utc) AS priority,
    MIN(IFF(event_type = 'OPENED', event_timestamp_utc, NULL)) AS opened_timestamp,
    MAX(IFF(event_type = 'CLOSED', event_timestamp_utc, NULL)) AS closed_timestamp,
    MAX_BY(event_type, event_timestamp_utc) AS last_event_type,
    SUM(IFF(event_type = 'WORK_COMPLETED', COALESCE(labor_minutes, 0), 0)) AS total_labor_minutes,
    COUNT(*) AS event_count,
    COUNT_IF(event_type = 'REOPENED') AS reopen_count,
    MAX(updated_at_utc) AS last_updated_timestamp
  FROM SILVER_WORK_ORDER_EVENTS
  GROUP BY work_order_id
)
SELECT work_order_id, client_id, store_id, technician_id, priority, opened_timestamp, closed_timestamp,
  CASE last_event_type
    WHEN 'CLOSED' THEN 'CLOSED' WHEN 'REOPENED' THEN 'REOPENED' WHEN 'OPENED' THEN 'OPEN'
    WHEN 'WORK_COMPLETED' THEN 'COMPLETED' ELSE last_event_type END AS current_status,
  total_labor_minutes, event_count, reopen_count,
  IFF(opened_timestamp IS NOT NULL AND closed_timestamp >= opened_timestamp,
      DATEDIFF('second', opened_timestamp, closed_timestamp) / 3600.0, NULL) AS resolution_hours,
  last_updated_timestamp
FROM per_work_order;
