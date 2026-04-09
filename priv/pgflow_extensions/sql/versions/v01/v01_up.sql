-- Tracking view for ecto_evolver version management
CREATE OR REPLACE VIEW $SCHEMA$.extensions_version AS SELECT 1 AS placeholder;

--SPLIT--

-- Drop any pre-existing functions that may have different param names
DROP FUNCTION IF EXISTS $SCHEMA$.get_flow_input(uuid);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.flow_exists(text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_step_output(uuid, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.register_worker(uuid, text, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.mark_worker_stopped(uuid);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.recover_stalled_tasks(double precision);

--SPLIT--

-- READ: Get flow run input data
CREATE FUNCTION $SCHEMA$.get_flow_input(p_run_id uuid)
RETURNS jsonb
LANGUAGE sql STABLE
SECURITY INVOKER
SET search_path = ''
AS $$
  SELECT input FROM pgflow.runs WHERE run_id = p_run_id;
$$;

--SPLIT--

-- READ: Check if flow exists
CREATE FUNCTION $SCHEMA$.flow_exists(p_flow_slug text)
RETURNS boolean
LANGUAGE sql STABLE
SECURITY INVOKER
SET search_path = ''
AS $$
  SELECT EXISTS(SELECT 1 FROM pgflow.flows WHERE flow_slug = p_flow_slug);
$$;

--SPLIT--

-- READ: Get step output
CREATE FUNCTION $SCHEMA$.get_step_output(p_run_id uuid, p_step_slug text)
RETURNS jsonb
LANGUAGE sql STABLE
SECURITY INVOKER
SET search_path = ''
AS $$
  SELECT output FROM pgflow.step_states WHERE run_id = p_run_id AND step_slug = p_step_slug;
$$;

--SPLIT--

-- WRITE: Register or heartbeat a worker
CREATE FUNCTION $SCHEMA$.register_worker(
  p_worker_id uuid, p_queue_name text, p_function_name text
)
RETURNS void
LANGUAGE sql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
  INSERT INTO pgflow.workers (worker_id, queue_name, function_name, started_at, last_heartbeat_at)
  VALUES (p_worker_id, p_queue_name, p_function_name, NOW(), NOW())
  ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat_at = NOW();
$$;

--SPLIT--

-- WRITE: Mark worker as stopped
CREATE FUNCTION $SCHEMA$.mark_worker_stopped(p_worker_id uuid)
RETURNS void
LANGUAGE sql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
  UPDATE pgflow.workers SET stopped_at = clock_timestamp() WHERE worker_id = p_worker_id;
$$;

--SPLIT--

-- WRITE: Recover stalled tasks + reset pgmq visibility
CREATE FUNCTION $SCHEMA$.recover_stalled_tasks(p_stale_threshold double precision)
RETURNS TABLE(recovered_count bigint, vt_batches bigint)
LANGUAGE sql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
  WITH stalled AS (
    UPDATE pgflow.step_tasks
    SET status = 'queued', started_at = NULL, last_worker_id = NULL
    WHERE status = 'started'
      AND started_at < NOW() - make_interval(secs => p_stale_threshold)
    RETURNING flow_slug, message_id
  ),
  vt_reset AS MATERIALIZED (
    SELECT pgflow.set_vt_batch(
      s.flow_slug,
      array_agg(s.message_id),
      array_agg(0::integer)
    )
    FROM stalled s
    WHERE s.message_id IS NOT NULL
    GROUP BY s.flow_slug
  )
  SELECT
    (SELECT count(*) FROM stalled)::bigint AS recovered_count,
    (SELECT count(*) FROM vt_reset)::bigint AS vt_batches;
$$;

--SPLIT--

-- SCHEMA EXTENSION: flow_type column on pgflow.flows
-- This column is an Elixir-specific extension NOT present in the upstream
-- TypeScript pgflow project. It distinguishes background jobs (single-step
-- flows) from multi-step DAG workflows in the dashboard.
-- Default 'flow' ensures backward compatibility with existing flow records.
ALTER TABLE pgflow.flows
  ADD COLUMN IF NOT EXISTS flow_type text NOT NULL DEFAULT 'flow';

--SPLIT--

-- Drop any existing flow_type constraint (may have been created with fewer values)
ALTER TABLE pgflow.flows
  DROP CONSTRAINT IF EXISTS flow_type_is_valid;

--SPLIT--

-- Add constraint allowing flow types (cron scheduling is now an option on flow/job, not a separate type)
ALTER TABLE pgflow.flows
  ADD CONSTRAINT flow_type_is_valid CHECK (flow_type IN ('flow', 'job'));

--SPLIT--

-- Drop any pre-existing prune function (interval only signature)
DROP FUNCTION IF EXISTS $SCHEMA$.prune_data_older_than(interval);

--SPLIT--

-- Drop any pre-existing prune function (interval + text[] signature)
DROP FUNCTION IF EXISTS $SCHEMA$.prune_data_older_than(interval, text[]);

--SPLIT--

-- MAINTENANCE: Prune old run data
-- Deletes completed/failed runs and all associated data older than retention_interval
-- If p_flow_slugs is NULL or empty, prunes all flows (global cleanup)
-- If p_flow_slugs is provided, only prunes runs for those specific flows
CREATE FUNCTION $SCHEMA$.prune_data_older_than(
  p_retention_interval INTERVAL,
  p_flow_slugs TEXT[] DEFAULT NULL
) RETURNS TABLE(
  deleted_runs bigint,
  deleted_step_states bigint,
  deleted_step_tasks bigint,
  deleted_workers bigint
)
LANGUAGE plpgsql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  cutoff_timestamp TIMESTAMPTZ := NOW() - p_retention_interval;
  v_deleted_runs bigint := 0;
  v_deleted_step_states bigint := 0;
  v_deleted_step_tasks bigint := 0;
  v_deleted_workers bigint := 0;
  flow_record RECORD;
  archive_table TEXT;
  dynamic_sql TEXT;
  filter_flows BOOLEAN := (p_flow_slugs IS NOT NULL AND array_length(p_flow_slugs, 1) > 0);
BEGIN
  -- Delete old worker records (always global - not flow-specific)
  DELETE FROM pgflow.workers
  WHERE last_heartbeat_at < cutoff_timestamp;
  GET DIAGNOSTICS v_deleted_workers = ROW_COUNT;

  -- Delete PGMQ messages from active queues BEFORE deleting step_tasks
  FOR flow_record IN
    SELECT
      r.flow_slug,
      ARRAY_AGG(st.message_id) FILTER (WHERE st.message_id IS NOT NULL) as message_ids
    FROM pgflow.runs r
    JOIN pgflow.step_tasks st ON st.run_id = r.run_id
    WHERE (
      (r.completed_at IS NOT NULL AND r.completed_at < cutoff_timestamp) OR
      (r.failed_at IS NOT NULL AND r.failed_at < cutoff_timestamp)
    )
    AND (NOT filter_flows OR r.flow_slug = ANY(p_flow_slugs))
    GROUP BY r.flow_slug
  LOOP
    IF flow_record.message_ids IS NOT NULL AND array_length(flow_record.message_ids, 1) > 0 THEN
      PERFORM pgmq.delete(flow_record.flow_slug, flow_record.message_ids);
    END IF;
  END LOOP;

  -- Delete step_tasks for old runs
  DELETE FROM pgflow.step_tasks
  WHERE run_id IN (
    SELECT run_id FROM pgflow.runs
    WHERE ((completed_at IS NOT NULL AND completed_at < cutoff_timestamp)
       OR (failed_at IS NOT NULL AND failed_at < cutoff_timestamp))
    AND (NOT filter_flows OR flow_slug = ANY(p_flow_slugs))
  );
  GET DIAGNOSTICS v_deleted_step_tasks = ROW_COUNT;

  -- Delete step_states for old runs
  DELETE FROM pgflow.step_states
  WHERE run_id IN (
    SELECT run_id FROM pgflow.runs
    WHERE ((completed_at IS NOT NULL AND completed_at < cutoff_timestamp)
       OR (failed_at IS NOT NULL AND failed_at < cutoff_timestamp))
    AND (NOT filter_flows OR flow_slug = ANY(p_flow_slugs))
  );
  GET DIAGNOSTICS v_deleted_step_states = ROW_COUNT;

  -- Delete old runs
  DELETE FROM pgflow.runs
  WHERE ((completed_at IS NOT NULL AND completed_at < cutoff_timestamp)
     OR (failed_at IS NOT NULL AND failed_at < cutoff_timestamp))
  AND (NOT filter_flows OR flow_slug = ANY(p_flow_slugs));
  GET DIAGNOSTICS v_deleted_runs = ROW_COUNT;

  -- Prune archived PGMQ messages (only for specified flows, or all if no filter)
  FOR flow_record IN
    SELECT DISTINCT flow_slug FROM pgflow.flows
    WHERE (NOT filter_flows OR flow_slug = ANY(p_flow_slugs))
  LOOP
    archive_table := pgmq.format_table_name(flow_record.flow_slug, 'a');
    IF EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'pgmq' AND table_name = archive_table
    ) THEN
      dynamic_sql := format('DELETE FROM pgmq.%I WHERE archived_at < $1', archive_table);
      EXECUTE dynamic_sql USING cutoff_timestamp;
    END IF;
  END LOOP;

  RETURN QUERY SELECT v_deleted_runs, v_deleted_step_states, v_deleted_step_tasks, v_deleted_workers;
END;
$$;

--SPLIT--

-- PERFORMANCE: Indexes for dashboard time-range queries
-- These indexes optimize the dashboard's filtering by started_at and per-flow queries
CREATE INDEX IF NOT EXISTS idx_runs_started_at ON pgflow.runs(started_at DESC);

--SPLIT--

-- Composite index for per-flow queries (used by crons/jobs show pages with lateral joins)
CREATE INDEX IF NOT EXISTS idx_runs_flow_started ON pgflow.runs(flow_slug, started_at DESC);

--SPLIT--

-- Index for worker health queries (ORDER BY last_heartbeat_at in dashboard, used by health status determination)
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON pgflow.workers(last_heartbeat_at DESC);

--SPLIT--

-- Version tracking
COMMENT ON VIEW $SCHEMA$.extensions_version IS 'PgFlow version=1';
