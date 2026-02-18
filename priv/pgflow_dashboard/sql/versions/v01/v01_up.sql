-- PgFlowDashboard Version 1 - Up Migration
-- Creates the pgflow_dashboard schema with views, materialized views, and functions

-- Create the dashboard schema
CREATE SCHEMA IF NOT EXISTS $SCHEMA$;

--SPLIT--

-- View: runs_with_progress
-- Shows runs with calculated progress percentage
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

-- View: runs_view
-- Full runs view with flow_type for Ecto/LiveFilter queries
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

-- View: workers_with_load
-- Shows workers with health status and load metrics
-- Includes flow_type to distinguish between flow workers and job workers
CREATE OR REPLACE VIEW $SCHEMA$.workers_with_load AS
SELECT
  w.worker_id,
  w.queue_name AS flow_slug,
  COALESCE(f.flow_type, 'flow') AS flow_type,
  w.last_heartbeat_at,
  CASE
    WHEN w.last_heartbeat_at > NOW() - INTERVAL '30 seconds' THEN 'healthy'
    WHEN w.last_heartbeat_at > NOW() - INTERVAL '60 seconds' THEN 'stale'
    ELSE 'dead'
  END AS health_status,
  COALESCE(tasks.active_count, 0) AS active_tasks,
  COALESCE(tasks.completed_count, 0) AS completed_tasks_24h
FROM pgflow.workers w
LEFT JOIN pgflow.flows f ON f.flow_slug = w.queue_name
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) FILTER (WHERE st.status = 'started') AS active_count,
    COUNT(*) FILTER (
      WHERE st.status = 'completed'
      AND st.completed_at > NOW() - INTERVAL '24 hours'
    ) AS completed_count
  FROM pgflow.step_tasks st
  JOIN pgflow.step_states ss ON st.run_id = ss.run_id AND st.step_slug = ss.step_slug
  JOIN pgflow.runs r ON ss.run_id = r.run_id
  WHERE r.flow_slug = w.queue_name
) tasks ON true;

--SPLIT--

-- View: flow_stats
-- Shows flow-level statistics for the last 24 hours
-- Includes flow_type to distinguish between DAG workflows ('flow') and background jobs ('job')
CREATE OR REPLACE VIEW $SCHEMA$.flow_stats AS
SELECT
  f.flow_slug AS flow_slug,
  COALESCE(f.flow_type, 'flow') AS flow_type,
  f.opt_max_attempts,
  f.opt_base_delay,
  f.opt_timeout,
  COALESCE(stats.total_runs_24h, 0) AS total_runs_24h,
  COALESCE(stats.completed_runs_24h, 0) AS completed_runs_24h,
  COALESCE(stats.failed_runs_24h, 0) AS failed_runs_24h,
  CASE
    WHEN stats.total_runs_24h > 0
    THEN ROUND((stats.completed_runs_24h::numeric / stats.total_runs_24h) * 100, 1)
    ELSE 0
  END AS success_rate_24h,
  COALESCE(stats.avg_duration_ms, 0) AS avg_duration_ms,
  COALESCE(stats.p95_duration_ms, 0) AS p95_duration_ms,
  (SELECT COUNT(*) FROM pgflow.steps s WHERE s.flow_slug = f.flow_slug) AS step_count
FROM pgflow.flows f
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_runs_24h,
    COUNT(*) FILTER (WHERE r.status = 'completed') AS completed_runs_24h,
    COUNT(*) FILTER (WHERE r.status = 'failed') AS failed_runs_24h,
    AVG(EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000)
      FILTER (WHERE r.status = 'completed') AS avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (
      ORDER BY EXTRACT(EPOCH FROM (r.completed_at - r.started_at)) * 1000
    ) FILTER (WHERE r.status = 'completed') AS p95_duration_ms
  FROM pgflow.runs r
  WHERE r.flow_slug = f.flow_slug
    AND r.started_at > NOW() - INTERVAL '24 hours'
) stats ON true;

--SPLIT--

-- View: step_states_with_tasks
-- Shows step states with task counts and dependencies
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

-- Function: get_overview_metrics()
-- Returns real-time dashboard overview metrics
CREATE OR REPLACE FUNCTION $SCHEMA$.get_overview_metrics()
RETURNS TABLE (
  active_workers bigint,
  healthy_workers bigint,
  stale_workers bigint,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  running_runs bigint,
  avg_duration_ms numeric,
  queue_depth bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    (SELECT COUNT(*) FROM pgflow.workers
     WHERE last_heartbeat_at > NOW() - INTERVAL '5 minutes') AS active_workers,
    (SELECT COUNT(*) FROM pgflow.workers
     WHERE last_heartbeat_at > NOW() - INTERVAL '30 seconds') AS healthy_workers,
    (SELECT COUNT(*) FROM pgflow.workers
     WHERE last_heartbeat_at BETWEEN NOW() - INTERVAL '60 seconds'
                                 AND NOW() - INTERVAL '30 seconds') AS stale_workers,
    (SELECT COUNT(*) FROM pgflow.runs
     WHERE started_at > NOW() - INTERVAL '24 hours') AS total_runs_24h,
    (SELECT COUNT(*) FROM pgflow.runs
     WHERE status = 'completed'
       AND started_at > NOW() - INTERVAL '24 hours') AS completed_runs_24h,
    (SELECT COUNT(*) FROM pgflow.runs
     WHERE status = 'failed'
       AND started_at > NOW() - INTERVAL '24 hours') AS failed_runs_24h,
    (SELECT COUNT(*) FROM pgflow.runs
     WHERE status = 'started') AS running_runs,
    (SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000), 0)
     FROM pgflow.runs
     WHERE status = 'completed'
       AND started_at > NOW() - INTERVAL '24 hours') AS avg_duration_ms,
    (SELECT COUNT(*) FROM pgflow.step_tasks WHERE status = 'queued')::bigint AS queue_depth
$$;

--SPLIT--

-- =====================================================
-- Query Functions (15 total)
-- These provide a portable SQL API for dashboard data
-- =====================================================

-- Function: list_runs()
-- Lists runs with optional filters and cursor pagination
-- Uses composite cursor (started_at, run_id) to avoid skip/duplicate issues with identical timestamps
CREATE OR REPLACE FUNCTION $SCHEMA$.list_runs(
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

-- Function: count_runs()
-- Counts runs matching filters
CREATE OR REPLACE FUNCTION $SCHEMA$.count_runs(
  p_time_range_start timestamptz DEFAULT (NOW() - INTERVAL '24 hours'),
  p_flow_slug text DEFAULT NULL,
  p_status text DEFAULT NULL,
  p_flow_type text DEFAULT NULL
)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)
  FROM $SCHEMA$.runs_with_progress r
  JOIN pgflow.flows f ON r.flow_slug = f.flow_slug
  WHERE r.started_at > p_time_range_start
    AND (p_flow_slug IS NULL OR r.flow_slug = p_flow_slug)
    AND (p_status IS NULL OR r.status = p_status)
    AND (p_flow_type IS NULL OR COALESCE(f.flow_type, 'flow') = p_flow_type)
$$;

--SPLIT--

-- Function: get_run()
-- Gets a single run by ID
CREATE OR REPLACE FUNCTION $SCHEMA$.get_run(p_run_id uuid)
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

-- Function: get_adjacent_run()
-- Gets the next or previous run for navigation
-- Uses base pgflow.runs table (not runs_with_progress) for efficiency
-- since only run_id and started_at are needed for navigation
CREATE OR REPLACE FUNCTION $SCHEMA$.get_adjacent_run(
  p_run_id uuid,
  p_direction text  -- 'next' or 'prev'
)
RETURNS uuid
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_current_started_at timestamptz;
  v_result uuid;
BEGIN
  SELECT started_at INTO v_current_started_at
  FROM pgflow.runs WHERE run_id = p_run_id;

  IF v_current_started_at IS NULL THEN
    RETURN NULL;
  END IF;

  IF p_direction = 'next' THEN
    -- Next = older (DESC ordering means next is < current)
    SELECT run_id INTO v_result
    FROM pgflow.runs
    WHERE started_at < v_current_started_at
    ORDER BY started_at DESC
    LIMIT 1;
  ELSIF p_direction = 'prev' THEN
    -- Prev = newer (ASC ordering means prev is > current)
    SELECT run_id INTO v_result
    FROM pgflow.runs
    WHERE started_at > v_current_started_at
    ORDER BY started_at ASC
    LIMIT 1;
  END IF;

  RETURN v_result;
END;
$$;

--SPLIT--

-- Function: list_step_states()
-- Lists step states for a run
CREATE OR REPLACE FUNCTION $SCHEMA$.list_step_states(p_run_id uuid)
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

-- Function: list_step_tasks()
-- Lists tasks for a specific step
CREATE OR REPLACE FUNCTION $SCHEMA$.list_step_tasks(
  p_run_id uuid,
  p_step_slug text
)
RETURNS TABLE (
  task_index integer,
  status text,
  attempts_count integer,
  error_message text,
  started_at timestamptz,
  completed_at timestamptz,
  output jsonb
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    st.task_index, st.status, st.attempts_count, st.error_message,
    st.started_at, st.completed_at, st.output
  FROM pgflow.step_tasks st
  WHERE st.run_id = p_run_id AND st.step_slug = p_step_slug
  ORDER BY st.task_index ASC
$$;

--SPLIT--

-- Function: count_workers()
-- Counts workers matching filters
CREATE OR REPLACE FUNCTION $SCHEMA$.count_workers(
  p_flow_slug text DEFAULT NULL,
  p_health_status text DEFAULT NULL
)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)
  FROM $SCHEMA$.workers_with_load w
  WHERE (p_flow_slug IS NULL OR w.flow_slug = p_flow_slug)
    AND (p_health_status IS NULL OR w.health_status = p_health_status)
$$;

--SPLIT--

-- Function: list_workers()
-- Lists workers with optional filters and cursor pagination
-- Uses composite cursor (last_heartbeat_at, worker_id) to avoid skip/duplicate issues with identical timestamps
CREATE OR REPLACE FUNCTION $SCHEMA$.list_workers(
  p_flow_slug text DEFAULT NULL,
  p_health_status text DEFAULT NULL,
  p_limit integer DEFAULT NULL,
  p_cursor_worker_id uuid DEFAULT NULL
)
RETURNS TABLE (
  worker_id uuid,
  flow_slug text,
  flow_type text,
  last_heartbeat_at timestamptz,
  health_status text,
  active_tasks bigint,
  completed_tasks_24h bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    w.worker_id, w.flow_slug, w.flow_type, w.last_heartbeat_at,
    w.health_status, w.active_tasks, w.completed_tasks_24h
  FROM $SCHEMA$.workers_with_load w
  WHERE (p_flow_slug IS NULL OR w.flow_slug = p_flow_slug)
    AND (p_health_status IS NULL OR w.health_status = p_health_status)
    AND (p_cursor_worker_id IS NULL OR (w.last_heartbeat_at, w.worker_id) < (
      SELECT sub.last_heartbeat_at, sub.worker_id FROM $SCHEMA$.workers_with_load sub
      WHERE sub.worker_id = p_cursor_worker_id
    ))
  ORDER BY w.last_heartbeat_at DESC, w.worker_id DESC
  LIMIT p_limit
$$;

--SPLIT--

-- Function: get_worker()
-- Gets a single worker by ID
CREATE OR REPLACE FUNCTION $SCHEMA$.get_worker(p_worker_id uuid)
RETURNS TABLE (
  worker_id uuid,
  flow_slug text,
  flow_type text,
  last_heartbeat_at timestamptz,
  health_status text,
  active_tasks bigint,
  completed_tasks_24h bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    w.worker_id, w.flow_slug, w.flow_type, w.last_heartbeat_at,
    w.health_status, w.active_tasks, w.completed_tasks_24h
  FROM $SCHEMA$.workers_with_load w
  WHERE w.worker_id = p_worker_id
$$;

--SPLIT--

-- Function: list_worker_tasks()
-- Lists tasks processed by a worker
CREATE OR REPLACE FUNCTION $SCHEMA$.list_worker_tasks(
  p_worker_id uuid,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  run_id uuid,
  step_slug text,
  task_index integer,
  status text,
  attempts_count integer,
  completed_at timestamptz,
  error_message text
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    st.run_id, st.step_slug, st.task_index, st.status,
    st.attempts_count, st.completed_at, st.error_message
  FROM pgflow.step_tasks st
  WHERE st.last_worker_id = p_worker_id
  ORDER BY COALESCE(st.completed_at, st.started_at, st.queued_at) DESC
  LIMIT p_limit
$$;

--SPLIT--

-- Function: get_adjacent_worker()
-- Gets the next or previous worker for navigation
CREATE OR REPLACE FUNCTION $SCHEMA$.get_adjacent_worker(
  p_worker_id uuid,
  p_direction text  -- 'next' or 'prev'
)
RETURNS uuid
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_current_heartbeat_at timestamptz;
  v_result uuid;
BEGIN
  SELECT last_heartbeat_at INTO v_current_heartbeat_at
  FROM $SCHEMA$.workers_with_load WHERE worker_id = p_worker_id;

  IF v_current_heartbeat_at IS NULL THEN
    RETURN NULL;
  END IF;

  IF p_direction = 'next' THEN
    -- Next = older heartbeat (DESC ordering)
    SELECT worker_id INTO v_result
    FROM $SCHEMA$.workers_with_load
    WHERE last_heartbeat_at < v_current_heartbeat_at
    ORDER BY last_heartbeat_at DESC
    LIMIT 1;
  ELSIF p_direction = 'prev' THEN
    -- Prev = newer heartbeat (ASC ordering)
    SELECT worker_id INTO v_result
    FROM $SCHEMA$.workers_with_load
    WHERE last_heartbeat_at > v_current_heartbeat_at
    ORDER BY last_heartbeat_at ASC
    LIMIT 1;
  END IF;

  RETURN v_result;
END;
$$;

--SPLIT--

-- Function: count_flows()
-- Counts all flows (excludes jobs)
CREATE OR REPLACE FUNCTION $SCHEMA$.count_flows()
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)
  FROM $SCHEMA$.flow_stats f
  WHERE f.flow_type = 'flow'
$$;

--SPLIT--

-- Function: list_flows()
-- Lists all flows with statistics (excludes jobs), with optional pagination
CREATE OR REPLACE FUNCTION $SCHEMA$.list_flows(
  p_limit integer DEFAULT NULL,
  p_cursor_slug text DEFAULT NULL
)
RETURNS TABLE (
  flow_slug text,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  success_rate_24h numeric,
  avg_duration_ms numeric,
  p95_duration_ms numeric,
  step_count bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    f.flow_slug, f.opt_max_attempts, f.opt_base_delay, f.opt_timeout,
    f.total_runs_24h, f.completed_runs_24h, f.failed_runs_24h,
    f.success_rate_24h, f.avg_duration_ms, f.p95_duration_ms, f.step_count
  FROM $SCHEMA$.flow_stats f
  WHERE f.flow_type = 'flow'
    AND (p_cursor_slug IS NULL OR f.flow_slug > p_cursor_slug)
  ORDER BY f.flow_slug
  LIMIT p_limit
$$;

--SPLIT--

-- Function: count_jobs()
-- Counts all jobs
CREATE OR REPLACE FUNCTION $SCHEMA$.count_jobs()
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)
  FROM $SCHEMA$.flow_stats f
  WHERE f.flow_type = 'job'
$$;

--SPLIT--

-- Function: list_jobs()
-- Lists all jobs with statistics, with optional pagination
CREATE OR REPLACE FUNCTION $SCHEMA$.list_jobs(
  p_limit integer DEFAULT NULL,
  p_cursor_slug text DEFAULT NULL
)
RETURNS TABLE (
  flow_slug text,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  success_rate_24h numeric,
  avg_duration_ms numeric,
  p95_duration_ms numeric
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    f.flow_slug, f.opt_max_attempts, f.opt_base_delay, f.opt_timeout,
    f.total_runs_24h, f.completed_runs_24h, f.failed_runs_24h,
    f.success_rate_24h, f.avg_duration_ms, f.p95_duration_ms
  FROM $SCHEMA$.flow_stats f
  WHERE f.flow_type = 'job'
    AND (p_cursor_slug IS NULL OR f.flow_slug > p_cursor_slug)
  ORDER BY f.flow_slug
  LIMIT p_limit
$$;

--SPLIT--

-- Function: get_flow()
-- Gets a single flow's statistics
CREATE OR REPLACE FUNCTION $SCHEMA$.get_flow(p_flow_slug text)
RETURNS TABLE (
  flow_slug text,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  success_rate_24h numeric,
  avg_duration_ms numeric,
  p95_duration_ms numeric,
  step_count bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    f.flow_slug, f.opt_max_attempts, f.opt_base_delay, f.opt_timeout,
    f.total_runs_24h, f.completed_runs_24h, f.failed_runs_24h,
    f.success_rate_24h, f.avg_duration_ms, f.p95_duration_ms, f.step_count
  FROM $SCHEMA$.flow_stats f
  WHERE f.flow_slug = p_flow_slug
    AND f.flow_type = 'flow'
$$;

--SPLIT--

-- Function: get_job()
-- Gets a single job's statistics
CREATE OR REPLACE FUNCTION $SCHEMA$.get_job(p_flow_slug text)
RETURNS TABLE (
  flow_slug text,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  success_rate_24h numeric,
  avg_duration_ms numeric,
  p95_duration_ms numeric
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    f.flow_slug, f.opt_max_attempts, f.opt_base_delay, f.opt_timeout,
    f.total_runs_24h, f.completed_runs_24h, f.failed_runs_24h,
    f.success_rate_24h, f.avg_duration_ms, f.p95_duration_ms
  FROM $SCHEMA$.flow_stats f
  WHERE f.flow_slug = p_flow_slug
    AND f.flow_type = 'job'
$$;

--SPLIT--

-- Function: count_crons()
-- Counts all scheduled flows/jobs
CREATE OR REPLACE FUNCTION $SCHEMA$.count_crons()
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)
  FROM $SCHEMA$.flow_stats f
  INNER JOIN cron.job cj ON cj.jobname = 'pgflow:' || f.flow_slug
$$;

--SPLIT--

-- Function: list_crons()
-- Lists all scheduled flows/jobs (detected by presence in cron.job table), with optional pagination
CREATE OR REPLACE FUNCTION $SCHEMA$.list_crons(
  p_limit integer DEFAULT NULL,
  p_cursor_slug text DEFAULT NULL
)
RETURNS TABLE (
  flow_slug text,
  flow_type text,
  cron_expression text,
  is_active boolean,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  success_rate_24h numeric,
  avg_duration_ms numeric,
  p95_duration_ms numeric,
  last_run_at timestamptz,
  last_run_status text
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    f.flow_slug,
    f.flow_type,
    cj.schedule AS cron_expression,
    COALESCE(cj.active, true) AS is_active,
    f.opt_max_attempts,
    f.opt_base_delay,
    f.opt_timeout,
    f.total_runs_24h,
    f.completed_runs_24h,
    f.failed_runs_24h,
    f.success_rate_24h,
    f.avg_duration_ms,
    f.p95_duration_ms,
    lr.last_run_at,
    lr.last_run_status
  FROM $SCHEMA$.flow_stats f
  INNER JOIN cron.job cj ON cj.jobname = 'pgflow:' || f.flow_slug
  LEFT JOIN LATERAL (
    SELECT r.completed_at AS last_run_at, r.status AS last_run_status
    FROM pgflow.runs r
    WHERE r.flow_slug = f.flow_slug
    ORDER BY r.started_at DESC
    LIMIT 1
  ) lr ON true
  WHERE (p_cursor_slug IS NULL OR f.flow_slug > p_cursor_slug)
  ORDER BY f.flow_slug
  LIMIT p_limit
$$;

--SPLIT--

-- Function: get_cron()
-- Gets a single scheduled flow/job's statistics and schedule info
CREATE OR REPLACE FUNCTION $SCHEMA$.get_cron(p_flow_slug text)
RETURNS TABLE (
  flow_slug text,
  flow_type text,
  cron_expression text,
  is_active boolean,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  total_runs_24h bigint,
  completed_runs_24h bigint,
  failed_runs_24h bigint,
  success_rate_24h numeric,
  avg_duration_ms numeric,
  p95_duration_ms numeric,
  last_run_at timestamptz,
  last_run_status text
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    f.flow_slug,
    f.flow_type,
    cj.schedule AS cron_expression,
    COALESCE(cj.active, true) AS is_active,
    f.opt_max_attempts,
    f.opt_base_delay,
    f.opt_timeout,
    f.total_runs_24h,
    f.completed_runs_24h,
    f.failed_runs_24h,
    f.success_rate_24h,
    f.avg_duration_ms,
    f.p95_duration_ms,
    lr.last_run_at,
    lr.last_run_status
  FROM $SCHEMA$.flow_stats f
  INNER JOIN cron.job cj ON cj.jobname = 'pgflow:' || f.flow_slug
  LEFT JOIN LATERAL (
    SELECT r.completed_at AS last_run_at, r.status AS last_run_status
    FROM pgflow.runs r
    WHERE r.flow_slug = f.flow_slug
    ORDER BY r.started_at DESC
    LIMIT 1
  ) lr ON true
  WHERE f.flow_slug = p_flow_slug
$$;

--SPLIT--

-- Function: list_flow_steps()
-- Lists flow steps with dependencies (for dependency graph)
CREATE OR REPLACE FUNCTION $SCHEMA$.list_flow_steps(p_flow_slug text)
RETURNS TABLE (
  step_slug text,
  step_type text,
  opt_max_attempts integer,
  opt_base_delay integer,
  opt_timeout integer,
  deps text[]
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    s.step_slug,
    s.step_type,
    s.opt_max_attempts,
    s.opt_base_delay,
    s.opt_timeout,
    COALESCE(d.dep_slugs, ARRAY[]::text[]) AS deps
  FROM pgflow.steps s
  LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(dep.dep_slug) AS dep_slugs
    FROM pgflow.deps dep
    WHERE dep.flow_slug = s.flow_slug AND dep.step_slug = s.step_slug
  ) d ON true
  WHERE s.flow_slug = p_flow_slug
  ORDER BY s.step_slug
$$;

--SPLIT--

-- Function: get_run_history_grid()
-- Gets run history data for GitHub-style activity grid
CREATE OR REPLACE FUNCTION $SCHEMA$.get_run_history_grid(
  p_flow_slug text,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  run_id uuid,
  started_at timestamptz,
  step_slug text,
  status text,
  duration_ms numeric
)
LANGUAGE sql
STABLE
AS $$
  WITH recent_runs AS (
    SELECT r.run_id, r.started_at
    FROM pgflow.runs r
    WHERE r.flow_slug = p_flow_slug
    ORDER BY r.started_at DESC
    LIMIT p_limit
  ),
  step_results AS (
    SELECT
      rr.run_id,
      rr.started_at,
      ss.step_slug,
      ss.status,
      ss.duration_ms
    FROM recent_runs rr
    LEFT JOIN $SCHEMA$.step_states_with_tasks ss ON rr.run_id = ss.run_id
  )
  SELECT sr.run_id, sr.started_at, sr.step_slug, sr.status, sr.duration_ms
  FROM step_results sr
  ORDER BY sr.started_at DESC, sr.step_slug
$$;

--SPLIT--

-- Version tracking comment
-- Used to track which migration version has been applied
COMMENT ON VIEW $SCHEMA$.runs_with_progress IS 'PgFlowDashboard version=1';
