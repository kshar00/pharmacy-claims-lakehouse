-- Dataset: dq_latest
-- The 34 data quality checks from the most recent pipeline run (silver.dq_results_latest view).
-- Sorted so anything that is not PASS shows first in the table widget.

SELECT
  check_id,
  category,
  table_name,
  check_name,
  severity,
  status,
  failed_rows,
  total_rows,
  round(failed_pct, 2)    AS failed_pct,
  max_failed_pct          AS tolerance_pct,
  run_id,
  run_ts
FROM portfolio.silver.dq_results_latest
ORDER BY CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END, check_id;
