-- Dataset: pipeline_audit_latest
-- Row counts and plan-paid dollars per layer for the most recent pipeline run, landing -> gold.
-- Lets a viewer see that the dashboard's numbers trace back to the files that were received.

SELECT
  CASE layer WHEN 'landing' THEN 1 WHEN 'bronze' THEN 2 WHEN 'silver' THEN 3 ELSE 4 END AS step,
  layer,
  table_name,
  row_count,
  plan_paid,
  note,
  run_id,
  audited_at
FROM portfolio.gold.pipeline_audit
WHERE run_id = (SELECT max_by(run_id, audited_at) FROM portfolio.gold.pipeline_audit)
ORDER BY step, table_name;
