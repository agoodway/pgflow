-- PgFlowDashboard Version 2 - Down Migration
-- Restores v01 definitions of objects replaced in v02.
--
-- v02_up.sql only ever touches: runs_with_progress, runs_view,
-- step_states_with_tasks (all via CREATE OR REPLACE VIEW, append-only), and
-- list_runs/get_run/list_step_states (DROP + CREATE, because their
-- RETURNS TABLE shape grew a column - Postgres doesn't allow
-- CREATE OR REPLACE FUNCTION to change the return type). count_runs and
-- get_run_history_grid are never touched by v02_up.sql, so down must not
-- drop or recreate them either - this mirrors v02_up.sql exactly rather
-- than re-deriving v01 from scratch.
--
-- Dropping the 3 views below (needed to shrink them back to their v01 column
-- set, which CREATE OR REPLACE VIEW cannot do) does NOT require dropping
-- count_runs/get_run_history_grid first: plain LANGUAGE sql functions do not
-- register a hard pg_depend dependency on views referenced in their body, so
-- DROP VIEW succeeds even while those functions still exist (they simply
-- fail if called before the view is recreated below, which happens
-- immediately after).

-- Drop functions whose signatures changed (RETURNS TABLE shape grew a column)
DROP FUNCTION IF EXISTS $SCHEMA$.list_runs(timestamptz, text, text, integer, uuid, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_run(uuid);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_step_states(uuid);

--SPLIT--

DROP VIEW IF EXISTS $SCHEMA$.runs_view;

--SPLIT--

DROP VIEW IF EXISTS $SCHEMA$.runs_with_progress;

--SPLIT--

DROP VIEW IF EXISTS $SCHEMA$.step_states_with_tasks;

--SPLIT--

-- Restore v01: runs_with_progress
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
    THEN ROUND((progress.completed_steps::numeric / progress.total_steps) * 100, 1)
    ELSE 0
  END AS progress_percent
FROM pgflow.runs r
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_steps,
    COUNT(*) FILTER (WHERE ss.status = 'completed') AS completed_steps,
    COUNT(*) FILTER (WHERE ss.status = 'failed') AS failed_steps
  FROM pgflow.step_states ss
  WHERE ss.run_id = r.run_id
) progress ON true;

--SPLIT--

-- Restore v01: runs_view
CREATE OR REPLACE VIEW $SCHEMA$.runs_view AS
SELECT
  r.run_id,
  r.flow_slug,
  COALESCE(f.flow_type, 'flow') AS flow_type,
  r.status,
  r.input,
  r.output,
  r.started_at,
  r.completed_at,
  r.duration_ms,
  r.total_steps,
  r.completed_steps,
  r.failed_steps,
  r.progress_percent
FROM $SCHEMA$.runs_with_progress r
LEFT JOIN pgflow.flows f ON r.flow_slug = f.flow_slug;

--SPLIT--

-- Restore v01: step_states_with_tasks
CREATE OR REPLACE VIEW $SCHEMA$.step_states_with_tasks AS
SELECT
  ss.run_id,
  ss.flow_slug,
  ss.step_slug,
  ss.status,
  ss.remaining_deps,
  ss.remaining_tasks,
  ss.started_at,
  ss.completed_at,
  EXTRACT(EPOCH FROM (COALESCE(ss.completed_at, NOW()) - ss.started_at)) * 1000 AS duration_ms,
  COALESCE(tasks.total_tasks, 0) AS total_tasks,
  COALESCE(tasks.completed_tasks, 0) AS completed_tasks,
  COALESCE(tasks.failed_tasks, 0) AS failed_tasks,
  s.step_type,
  COALESCE(deps.dep_slugs, ARRAY[]::text[]) AS deps
FROM pgflow.step_states ss
JOIN pgflow.steps s ON ss.flow_slug = s.flow_slug AND ss.step_slug = s.step_slug
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_tasks,
    COUNT(*) FILTER (WHERE st.status = 'completed') AS completed_tasks,
    COUNT(*) FILTER (WHERE st.status = 'failed') AS failed_tasks
  FROM pgflow.step_tasks st
  WHERE st.run_id = ss.run_id AND st.step_slug = ss.step_slug
) tasks ON true
LEFT JOIN LATERAL (
  SELECT ARRAY_AGG(d.dep_slug) AS dep_slugs
  FROM pgflow.deps d
  WHERE d.flow_slug = ss.flow_slug AND d.step_slug = ss.step_slug
) deps ON true;

--SPLIT--

-- Restore v01: list_runs
CREATE FUNCTION $SCHEMA$.list_runs(
  p_time_range_start timestamptz DEFAULT (NOW() - INTERVAL '24 hours'),
  p_flow_slug text DEFAULT NULL,
  p_status text DEFAULT NULL,
  p_limit integer DEFAULT 50,
  p_cursor_run_id uuid DEFAULT NULL,
  p_flow_type text DEFAULT NULL
)
RETURNS TABLE (
  run_id uuid,
  flow_slug text,
  flow_type text,
  status text,
  input jsonb,
  output jsonb,
  started_at timestamptz,
  completed_at timestamptz,
  duration_ms numeric,
  total_steps bigint,
  completed_steps bigint,
  failed_steps bigint,
  progress_percent numeric
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    r.run_id, r.flow_slug, COALESCE(f.flow_type, 'flow') AS flow_type, r.status, r.input, r.output,
    r.started_at, r.completed_at, r.duration_ms,
    r.total_steps, r.completed_steps, r.failed_steps, r.progress_percent
  FROM $SCHEMA$.runs_with_progress r
  JOIN pgflow.flows f ON r.flow_slug = f.flow_slug
  WHERE r.started_at > p_time_range_start
    AND (p_flow_slug IS NULL OR r.flow_slug = p_flow_slug)
    AND (p_status IS NULL OR r.status = p_status)
    AND (p_flow_type IS NULL OR COALESCE(f.flow_type, 'flow') = p_flow_type)
    AND (p_cursor_run_id IS NULL OR (r.started_at, r.run_id) < (
      SELECT sub.started_at, sub.run_id FROM pgflow.runs sub WHERE sub.run_id = p_cursor_run_id
    ))
  ORDER BY r.started_at DESC, r.run_id DESC
  LIMIT p_limit
$$;

--SPLIT--

-- Restore v01: get_run
CREATE FUNCTION $SCHEMA$.get_run(p_run_id uuid)
RETURNS TABLE (
  run_id uuid,
  flow_slug text,
  flow_type text,
  status text,
  input jsonb,
  output jsonb,
  started_at timestamptz,
  completed_at timestamptz,
  duration_ms numeric,
  total_steps bigint,
  completed_steps bigint,
  failed_steps bigint,
  progress_percent numeric
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    r.run_id, r.flow_slug, COALESCE(f.flow_type, 'flow') AS flow_type, r.status, r.input, r.output,
    r.started_at, r.completed_at, r.duration_ms,
    r.total_steps, r.completed_steps, r.failed_steps, r.progress_percent
  FROM $SCHEMA$.runs_with_progress r
  JOIN pgflow.flows f ON r.flow_slug = f.flow_slug
  WHERE r.run_id = p_run_id
$$;

--SPLIT--

-- Restore v01: list_step_states
CREATE FUNCTION $SCHEMA$.list_step_states(p_run_id uuid)
RETURNS TABLE (
  run_id uuid,
  flow_slug text,
  step_slug text,
  status text,
  remaining_deps integer,
  remaining_tasks integer,
  started_at timestamptz,
  completed_at timestamptz,
  duration_ms numeric,
  total_tasks bigint,
  completed_tasks bigint,
  failed_tasks bigint,
  step_type text,
  deps text[]
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    s.run_id, s.flow_slug, s.step_slug, s.status,
    s.remaining_deps, s.remaining_tasks,
    s.started_at, s.completed_at, s.duration_ms,
    s.total_tasks, s.completed_tasks, s.failed_tasks,
    s.step_type, s.deps
  FROM $SCHEMA$.step_states_with_tasks s
  WHERE s.run_id = p_run_id
  ORDER BY s.started_at ASC NULLS LAST
$$;

--SPLIT--

-- Restore v01 version tracking comment
COMMENT ON VIEW $SCHEMA$.runs_with_progress IS 'PgFlowDashboard version=1';
