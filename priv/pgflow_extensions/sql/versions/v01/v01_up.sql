-- Tracking view for pg_evolver version management
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

-- Version tracking
COMMENT ON VIEW $SCHEMA$.extensions_version IS 'PgFlow version=1';
