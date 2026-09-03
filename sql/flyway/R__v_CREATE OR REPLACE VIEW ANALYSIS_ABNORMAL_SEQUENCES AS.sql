CREATE OR REPLACE VIEW ANALYSIS_TECHNICIAN_CLOSE_RANK AS
SELECT technician_id, COUNT(*) AS closed_work_order_count,
       DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS close_rank
FROM WORK_ORDER_SUMMARY
WHERE current_status = 'CLOSED' AND technician_id IS NOT NULL
GROUP BY technician_id;
