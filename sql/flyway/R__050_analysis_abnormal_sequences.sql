CREATE OR REPLACE VIEW ANALYSIS_ABNORMAL_SEQUENCES AS
SELECT work_order_id, opened_timestamp, closed_timestamp, current_status, reopen_count,
  CASE WHEN opened_timestamp IS NULL THEN 'missing OPENED event'
       WHEN closed_timestamp IS NULL AND current_status = 'CLOSED' THEN 'CLOSED status without valid close timestamp'
       WHEN closed_timestamp < opened_timestamp THEN 'close precedes open'
       WHEN reopen_count > 0 AND current_status = 'CLOSED' THEN 'reopen history requires review'
       WHEN event_count = 1 THEN 'single-event work order' END AS anomaly_reason
FROM WORK_ORDER_SUMMARY
WHERE opened_timestamp IS NULL OR (closed_timestamp IS NOT NULL AND closed_timestamp < opened_timestamp)
   OR (reopen_count > 0 AND current_status = 'CLOSED') OR event_count = 1;
