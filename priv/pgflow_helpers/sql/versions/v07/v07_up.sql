-- WRITE: Require core V03 before the 0.17.2 claim overlay.
DO $pgflow_helpers_v07_prereq$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'pgflow'
      AND table_name = 'flows'
      AND column_name = 'queue_mode'
  ) OR to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb,text,jsonb)') IS NULL THEN
    RAISE EXCEPTION 'PgFlow helpers V07 requires core V03. Run PgFlow.Migration.up/0 first.';
  END IF;
END
$pgflow_helpers_v07_prereq$;

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.start_tasks(text, bigint[], uuid, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.start_tasks(text, bigint[], uuid);

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$."start_tasks" ("flow_slug" text, "msg_ids" bigint[], "worker_id" uuid, "queue_name" text, "step_slug" text DEFAULT NULL::text) RETURNS SETOF $SCHEMA$."step_task_record" LANGUAGE plpgsql SET "search_path" = '' SET "plan_cache_mode" = 'force_custom_plan' AS $$
DECLARE
  v_queue_mode text;
  v_route_exists boolean;
BEGIN
  -- One combined pre-claim probe (#651): the queue mode and, in step mode,
  -- whether the exact (flow_slug, step_slug, queue_name) route persists.
  -- Keeping this to a single statement avoids an extra lookup on the claim
  -- hot path without weakening the exact-selector checks below; in flow
  -- mode with no selector the route EXISTS() is not evaluated at all.
  SELECT flow.queue_mode, EXISTS (
    SELECT 1
    FROM pgflow.steps AS s
    WHERE s.flow_slug = start_tasks.flow_slug
      AND s.step_slug = start_tasks.step_slug
      AND s.queue_name = start_tasks.queue_name
  )
  INTO v_queue_mode, v_route_exists
  FROM pgflow.flows AS flow
  WHERE flow.flow_slug = start_tasks.flow_slug;

  IF v_queue_mode IS NULL THEN
    -- Unknown flow: nothing claimable (preserves the empty-result behavior)
    RETURN;
  END IF;

  IF v_queue_mode = 'step' THEN
    IF start_tasks.step_slug IS NULL THEN
      RAISE EXCEPTION
        'Flow "%" uses per-step queues: an exact step_slug is required to claim tasks.',
        start_tasks.flow_slug
        USING detail = format(
          'Queue "%s" is a private step queue; a claim without a step selector could mix steps.',
          start_tasks.queue_name
        ),
        hint = 'Pass the exact step_slug of the polled step; direct SQL cannot obtain flow-wide claims in step mode.';
    END IF;

    IF NOT v_route_exists THEN
      RAISE EXCEPTION
        'Step "%" does not route to queue "%" in flow "%".',
        start_tasks.step_slug, start_tasks.queue_name, start_tasks.flow_slug
        USING detail = 'The step selector must match a persisted step route (flow_slug, step_slug, queue_name) exactly.',
        hint = 'Poll the queue recorded for this step and pass its exact canonical name and spelling.';
    END IF;
  ELSIF start_tasks.step_slug IS NOT NULL THEN
    RAISE EXCEPTION
      'Flow "%" uses the default flow queue: a step selector is not allowed.',
      start_tasks.flow_slug
      USING detail = format(
        'Step "%s" was supplied, but flow queue mode has no per-step queues.',
        start_tasks.step_slug
      ),
      hint = 'Omit step_slug to claim flow-wide tasks.';
  END IF;

  RETURN QUERY
  with task_candidates as (
    select
      task.flow_slug,
      task.run_id,
      task.step_slug,
      task.task_index,
      task.message_id
    from pgflow.step_tasks as task
    join pgflow.runs r on r.run_id = task.run_id
    where task.flow_slug = start_tasks.flow_slug
      and task.queue_name = start_tasks.queue_name
      and (start_tasks.step_slug IS NULL OR task.step_slug = start_tasks.step_slug)
      and task.message_id = any(msg_ids)
      and task.status = 'queued'
      and r.status = 'started'
      and exists (
        select 1
        from pgflow.step_states ss
        where ss.run_id = task.run_id
          and ss.step_slug = task.step_slug
          and ss.status = 'started'
      )
  ),
  -- Claim rows with a guarded update and return only what was actually
  -- claimed. A concurrent skip can win the row lock between the candidate
  -- select and this update; the status = 'queued' recheck then claims nothing,
  -- so no stale candidate row must escape to the worker (#638).
  tasks as (
    update pgflow.step_tasks
    set
      attempts_count = attempts_count + 1,
      status = 'started',
      started_at = now(),
      last_worker_id = worker_id
    from task_candidates as candidate
    where step_tasks.message_id = candidate.message_id
      and step_tasks.flow_slug = candidate.flow_slug
      and step_tasks.queue_name = start_tasks.queue_name
      and step_tasks.status = 'queued'
    returning
      step_tasks.flow_slug,
      step_tasks.run_id,
      step_tasks.step_slug,
      step_tasks.task_index,
      step_tasks.message_id,
      step_tasks.attempts_count
  ),
  runs as (
    select
      r.run_id,
      r.input
    from pgflow.runs r
    where r.run_id in (select run_id from tasks)
  ),
  deps as (
    select
      st.run_id,
      st.step_slug,
      dep.dep_slug,
      -- Read output directly from step_states (already aggregated by writers)
      dep_state.output as dep_output
    from tasks st
    join pgflow.deps dep on dep.flow_slug = st.flow_slug and dep.step_slug = st.step_slug
    join pgflow.step_states dep_state on
      dep_state.run_id = st.run_id and
      dep_state.step_slug = dep.dep_slug and
      dep_state.status = 'completed'  -- Only include completed deps (not skipped)
  ),
  deps_outputs as (
    select
      d.run_id,
      d.step_slug,
      jsonb_object_agg(d.dep_slug, d.dep_output) as deps_output,
      count(*) as dep_count
    from deps d
    group by d.run_id, d.step_slug
  ),
  timeouts as (
    select
      task.message_id,
      task.flow_slug,
      coalesce(step.opt_timeout, flow.opt_timeout) + 2 as vt_delay
    from tasks task
    join pgflow.flows flow on flow.flow_slug = task.flow_slug
    join pgflow.steps step on step.flow_slug = task.flow_slug and step.step_slug = task.step_slug
  ),
  -- Batch update visibility timeouts for all messages.
  -- The final statement must force this CTE to run: an unreferenced SELECT
  -- CTE is not guaranteed to execute, which would leave a claimed task with
  -- only the shorter initial PGMQ read visibility (#656).
  visibility_reset as (
    select pgflow.set_vt_batch(
      start_tasks.queue_name,
      array_agg(t.message_id order by t.message_id),
      array_agg(t.vt_delay order by t.message_id)
    )
    from timeouts t
  ),
  -- Force execution of the visibility_reset CTE (same pattern as
  -- requeue_stalled_tasks) and guard completeness: set_vt_batch updates
  -- only queue rows it finds, so fewer returned rows than claimed tasks
  -- means a visibility extension did not run (#656). SQL functions cannot
  -- RAISE, so the mismatch branch casts a descriptive message to int4:
  -- the cast error fails the whole statement, rolling back the task
  -- transition and attempt increment, and returns nothing.
  _vr as (
    select case
      when updated.updated_count = claimed.claimed_count then updated.updated_count
      else format(
          'start_tasks(): visibility updated %s of %s claimed messages',
          updated.updated_count,
          claimed.claimed_count
        )::int4
    end as visibility_updates
    from (select count(*) as updated_count from visibility_reset) as updated
    cross join (select count(*) as claimed_count from tasks) as claimed
  )
  select
    st.flow_slug,
    st.run_id,
    st.step_slug,
    -- ==========================================
    -- INPUT CONSTRUCTION LOGIC
    -- ==========================================
    -- This nested CASE statement determines how to construct the input
    -- for each task based on the step type (map vs non-map).
    --
    -- The fundamental difference:
    -- - Map steps: Receive RAW array elements (e.g., just 42 or "hello")
    -- - Non-map steps: Receive structured objects with named keys
    --                  (e.g., {"run": {...}, "dependency1": {...}})
    -- ==========================================
    CASE
      -- -------------------- MAP STEPS --------------------
      -- Map steps process arrays element-by-element.
      -- Each task receives ONE element from the array at its task_index position.
      WHEN step.step_type = 'map' THEN
        -- Map steps get raw array elements without any wrapper object
        CASE
          -- ROOT MAP: Gets array from run input
          -- Example: run input = [1, 2, 3]
          --          task 0 gets: 1
          --          task 1 gets: 2
          --          task 2 gets: 3
          WHEN step.deps_count = 0 THEN
            -- Root map (deps_count = 0): no dependencies, reads from run input.
            -- Extract the element at task_index from the run's input array.
            -- Note: If run input is not an array, this will return NULL
            -- and the flow will fail (validated in start_flow).
            jsonb_array_element(r.input, st.task_index)

          -- DEPENDENT MAP: Gets array from its single dependency
          -- Example: dependency output = ["a", "b", "c"]
          --          task 0 gets: "a"
          --          task 1 gets: "b"
          --          task 2 gets: "c"
          ELSE
            -- Has dependencies (should be exactly 1 for map steps).
            -- Extract the element at task_index from the dependency's output array.
            --
            -- Why the subquery with jsonb_each?
            -- - The dependency outputs a raw array: [1, 2, 3]
            -- - deps_outputs aggregates it into: {"dep_name": [1, 2, 3]}
            -- - We need to unwrap and get just the array value
            -- - Map steps have exactly 1 dependency (enforced by add_step)
            -- - So jsonb_each will return exactly 1 row
            -- - We extract the 'value' which is the raw array [1, 2, 3]
            -- - Then get the element at task_index from that array
            (SELECT jsonb_array_element(value, st.task_index)
            FROM jsonb_each(dep_out.deps_output)
            LIMIT 1)
        END

      -- -------------------- NON-MAP STEPS --------------------
      -- Regular (non-map) steps receive dependency outputs as a structured object.
      -- Root steps (no dependencies) get empty object - they access flow_input via context.
      -- Dependent steps get only their dependency outputs.
      ELSE
        -- Non-map steps get structured input with dependency keys only
        -- Example for dependent step: {
        --   "step1": {"output": "from_step1"},
        --   "step2": {"output": "from_step2"}
        -- }
        -- Example for root step: {}
        --
        -- Note: flow_input is available separately in the returned record
        -- for workers to access via context.flowInput
        coalesce(dep_out.deps_output, '{}'::jsonb)
    END as input,
    st.message_id as msg_id,
    st.task_index as task_index,
    -- flow_input: Original run input for worker context
    -- Only included for root non-map steps to avoid data duplication.
    -- Root map steps: flowInput IS the array, useless to include
    -- Dependent steps: lazy load via ctx.flowInput when needed
    CASE
      WHEN step.step_type != 'map' AND step.deps_count = 0
      THEN r.input
      ELSE NULL
    END as flow_input,
    st.attempts_count
  from tasks st
  join runs r on st.run_id = r.run_id
  join pgflow.steps step on
    step.flow_slug = st.flow_slug and
    step.step_slug = st.step_slug
  left join deps_outputs dep_out on
    dep_out.run_id = st.run_id and
    dep_out.step_slug = st.step_slug
  cross join _vr
  where _vr.visibility_updates >= 0;
END;
$$

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.register_worker(uuid, text, text);

--SPLIT--

-- WRITE: Register or heartbeat a worker, stamping the runtime version.
CREATE FUNCTION $SCHEMA$.register_worker(
  p_worker_id uuid,
  p_queue_name text,
  p_function_name text,
  p_pgflow_version text
)
RETURNS void
LANGUAGE sql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
  INSERT INTO pgflow.workers (
    worker_id, queue_name, function_name, started_at, last_heartbeat_at, pgflow_version
  )
  VALUES (p_worker_id, p_queue_name, p_function_name, NOW(), NOW(), p_pgflow_version)
  ON CONFLICT (worker_id) DO UPDATE
    SET last_heartbeat_at = NOW(), pgflow_version = EXCLUDED.pgflow_version;
$$;

--SPLIT--

-- WRITE: Delete one run's lifecycle rows and its exact queue messages.
-- Lock order: run, then every task row of the run (NULL message ids included),
-- then queue rows. Claim and recovery lock task rows before queue rows, so
-- taking every task lock before touching a queue keeps them from forming a
-- lock cycle with this function. Raises SQLSTATE PGF01 for an invalid
-- persisted flow slug, before anything is deleted.
CREATE OR REPLACE FUNCTION $SCHEMA$.delete_run(p_run_id uuid)
RETURNS void
LANGUAGE plpgsql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_flow_slug text;
  v_route record;
  v_prefix text;
  v_table text;
BEGIN
  SELECT r.flow_slug INTO v_flow_slug
  FROM pgflow.runs AS r
  WHERE r.run_id = p_run_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  IF NOT coalesce(pgflow.is_valid_slug(v_flow_slug), false) THEN
    RAISE EXCEPTION USING
      ERRCODE = 'PGF01',
      MESSAGE = format('pgflow.delete_run: invalid flow slug %L for run %s', v_flow_slug, p_run_id);
  END IF;

  PERFORM 1
  FROM (
    SELECT st.task_index
    FROM pgflow.step_tasks AS st
    WHERE st.run_id = p_run_id
    ORDER BY st.step_slug, st.task_index
    FOR UPDATE
  ) AS locked_tasks;

  FOR v_route IN
    SELECT st.queue_name, array_agg(st.message_id ORDER BY st.message_id) AS message_ids
    FROM pgflow.step_tasks AS st
    WHERE st.run_id = p_run_id
      AND st.message_id IS NOT NULL
    GROUP BY st.queue_name
    ORDER BY st.queue_name
  LOOP
    FOREACH v_prefix IN ARRAY ARRAY['q', 'a'] LOOP
      v_table := format('pgmq.%I', pgmq.format_table_name(v_route.queue_name, v_prefix));

      IF to_regclass(v_table) IS NOT NULL THEN
        EXECUTE format('DELETE FROM %s WHERE msg_id = ANY($1)', v_table)
        USING v_route.message_ids;
      END IF;
    END LOOP;
  END LOOP;

  DELETE FROM pgflow.step_tasks WHERE run_id = p_run_id;
  DELETE FROM pgflow.step_states WHERE run_id = p_run_id;
  DELETE FROM pgflow.runs WHERE run_id = p_run_id;
END;
$$;

--SPLIT--

-- WRITE: Queue-aware pruning with the delete_run lock order: lock the eligible
-- runs (by run_id), then their task rows, then clean queues, then delete the
-- locked set. Signature, filters, worker and archive sweeps, and the return
-- shape match helpers V05.
CREATE OR REPLACE FUNCTION $SCHEMA$.prune_data_older_than(
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
  v_run_ids uuid[];
  task_record RECORD;
  route_record RECORD;
  archive_table TEXT;
  dynamic_sql TEXT;
  filter_flows BOOLEAN := (p_flow_slugs IS NOT NULL AND array_length(p_flow_slugs, 1) > 0);
BEGIN
  DELETE FROM pgflow.workers
  WHERE last_heartbeat_at < cutoff_timestamp;
  GET DIAGNOSTICS v_deleted_workers = ROW_COUNT;

  SELECT coalesce(array_agg(locked_runs.run_id ORDER BY locked_runs.run_id), '{}'::uuid[])
  INTO v_run_ids
  FROM (
    SELECT r.run_id
    FROM pgflow.runs AS r
    WHERE (
      (r.completed_at IS NOT NULL AND r.completed_at < cutoff_timestamp) OR
      (r.failed_at IS NOT NULL AND r.failed_at < cutoff_timestamp)
    )
    AND (NOT filter_flows OR r.flow_slug = ANY(p_flow_slugs))
    ORDER BY r.run_id
    FOR UPDATE
  ) AS locked_runs;

  PERFORM 1
  FROM (
    SELECT st.task_index
    FROM pgflow.step_tasks AS st
    WHERE st.run_id = ANY(v_run_ids)
    ORDER BY st.run_id, st.step_slug, st.task_index
    FOR UPDATE
  ) AS locked_tasks;

  FOR task_record IN
    SELECT
      st.queue_name,
      ARRAY_AGG(st.message_id) FILTER (WHERE st.message_id IS NOT NULL) AS message_ids
    FROM pgflow.step_tasks AS st
    WHERE st.run_id = ANY(v_run_ids)
    GROUP BY st.queue_name
    ORDER BY st.queue_name
  LOOP
    IF task_record.message_ids IS NOT NULL AND array_length(task_record.message_ids, 1) > 0 THEN
      PERFORM pgmq.delete(task_record.queue_name, task_record.message_ids);
    END IF;
  END LOOP;

  DELETE FROM pgflow.step_tasks WHERE run_id = ANY(v_run_ids);
  GET DIAGNOSTICS v_deleted_step_tasks = ROW_COUNT;

  DELETE FROM pgflow.step_states WHERE run_id = ANY(v_run_ids);
  GET DIAGNOSTICS v_deleted_step_states = ROW_COUNT;

  DELETE FROM pgflow.runs WHERE run_id = ANY(v_run_ids);
  GET DIAGNOSTICS v_deleted_runs = ROW_COUNT;

  FOR route_record IN
    SELECT DISTINCT queue_name FROM (
      SELECT queue_name FROM pgflow.steps
      WHERE NOT filter_flows OR flow_slug = ANY(p_flow_slugs)
      UNION
      SELECT lower(flow_slug) FROM pgflow.flows
      WHERE NOT filter_flows OR flow_slug = ANY(p_flow_slugs)
    ) routes
  LOOP
    archive_table := pgmq.format_table_name(route_record.queue_name, 'a');
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
