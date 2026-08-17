-- PgFlowDashboard Version 3 - Down Migration
-- Restores the v02 definition of runs_with_progress (duration_ms falls back to
-- NOW() for any run without a completed_at, failed runs included).
--
-- v03_up.sql only ever touches runs_with_progress, and only its duration_ms
-- expression - the column list is identical - so CREATE OR REPLACE VIEW is
-- enough in both directions and no dependent object (runs_view, list_runs,
-- get_run) has to be dropped or recreated.

-- Restore v02: runs_with_progress
CREATE OR REPLACE VIEW $SCHEMA$.runs_with_progress AS
SELECT
  r.run_id,
  r.flow_slug,
  r.status,
  r.input,
  r.output,
  r.started_at,
  r.completed_at,
  EXTRACT(EPOCH FROM (COALESCE(r.completed_at, NOW()) - r.started_at)) * 1000 AS duration_ms,
  COALESCE(progress.total_steps, 0) AS total_steps,
  COALESCE(progress.completed_steps, 0) AS completed_steps,
  COALESCE(progress.failed_steps, 0) AS failed_steps,
  CASE
    WHEN progress.total_steps > 0
    THEN ROUND(((progress.completed_steps + progress.skipped_steps)::numeric / progress.total_steps) * 100, 1)
    ELSE 0
  END AS progress_percent,
  COALESCE(progress.skipped_steps, 0) AS skipped_steps
FROM pgflow.runs r
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_steps,
    COUNT(*) FILTER (WHERE ss.status = 'completed') AS completed_steps,
    COUNT(*) FILTER (WHERE ss.status = 'failed') AS failed_steps,
    COUNT(*) FILTER (WHERE ss.status = 'skipped') AS skipped_steps
  FROM pgflow.step_states ss
  WHERE ss.run_id = r.run_id
) progress ON true;

--SPLIT--

-- Restore v02 version tracking comment
COMMENT ON VIEW $SCHEMA$.runs_with_progress IS 'PgFlowDashboard version=2';
