CREATE OR REPLACE VIEW ANALYSIS_TECHNICIAN_CLOSE_RANK AS
SELECT technician_id, COUNT(*) AS closed_work_order_count,
       DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS close_rank
FROM WORK_ORDER_SUMMARY
WHERE current_status = 'CLOSED' AND technician_id IS NOT NULL
GROUP BY technician_id;

CREATE OR REPLACE VIEW ANALYSIS_WEEKLY_COMPLETED AS
SELECT DATE_TRUNC('WEEK', closed_timestamp) AS week_start,
       COUNT(*) AS completed_work_orders,
       AVG(resolution_hours) AS average_resolution_hours
FROM WORK_ORDER_SUMMARY
WHERE current_status = 'CLOSED'
GROUP BY 1;

CREATE OR REPLACE VIEW ANALYSIS_ABNORMAL_SEQUENCES AS
SELECT work_order_id, opened_timestamp, closed_timestamp, current_status, reopen_count,
  CASE WHEN opened_timestamp IS NULL THEN 'missing OPENED event'
       WHEN closed_timestamp IS NULL AND current_status = 'CLOSED' THEN 'CLOSED status without valid close timestamp'
       WHEN closed_timestamp < opened_timestamp THEN 'close precedes open'
       WHEN reopen_count > 0 AND current_status = 'CLOSED' THEN 'reopen history requires review'
       WHEN event_count = 1 THEN 'single-event work order'
  END AS anomaly_reason
FROM WORK_ORDER_SUMMARY
WHERE opened_timestamp IS NULL
   OR (closed_timestamp IS NOT NULL AND closed_timestamp < opened_timestamp)
   OR (reopen_count > 0 AND current_status = 'CLOSED')
   OR event_count = 1;
