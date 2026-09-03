CREATE OR REPLACE VIEW ANALYSIS_WEEKLY_COMPLETED AS
SELECT DATE_TRUNC('WEEK', closed_timestamp) AS week_start, COUNT(*) AS completed_work_orders,
       AVG(resolution_hours) AS average_resolution_hours
FROM WORK_ORDER_SUMMARY
WHERE current_status = 'CLOSED'
GROUP BY 1;
