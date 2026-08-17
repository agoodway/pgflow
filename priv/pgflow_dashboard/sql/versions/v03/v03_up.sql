-- PgFlowDashboard Version 3 - Up Migration
-- Bounds a failed run's duration by failed_at instead of NOW()
--
-- v02 fixed the STEP-level formula (COALESCE(ss.completed_at, ss.skipped_at,
-- ss.failed_at, NOW())) but missed the RUN-level one: a failed run never gets
-- a completed_at, so `COALESCE(r.completed_at, NOW())` kept re-measuring
-- against the current time and the displayed duration grew without bound.
-- `pgflow.runs` has a `failed_at` column (and a CHECK that completed_at and
-- failed_at are never both set), so COALESCE(completed_at, failed_at, NOW())
-- terminalizes the duration and leaves only still-running runs on NOW().
--
-- Only runs_with_progress is replaced. runs_view, list_runs() and get_run()
-- all read duration_ms straight from this view, and the column list is
-- unchanged, so they pick up the fix without being recreated.

-- View: runs_with_progress
CREATE OR REPLACE VIEW $SCHEMA$.runs_with_progress AS
SELECT
  r.run_id,
  r.flow_slug,
  r.status,
  r.input,
  r.output,
  r.started_at,
  r.completed_at,
  EXTRACT(EPOCH FROM (COALESCE(r.completed_at, r.failed_at, NOW()) - r.started_at)) * 1000 AS duration_ms,
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

-- Version tracking comment
COMMENT ON VIEW $SCHEMA$.runs_with_progress IS 'PgFlowDashboard version=3';
