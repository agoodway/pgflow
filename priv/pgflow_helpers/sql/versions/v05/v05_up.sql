-- WRITE: Reconcile helpers with core V02 queue identity and the recorded
-- eight-column four-argument claim from the V02 bundle.
DO $pgflow_helpers_v05_prereq$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'pgflow'
      AND table_name = 'step_tasks'
      AND column_name = 'queue_name'
  ) THEN
    RAISE EXCEPTION 'PgFlow helpers V05 requires core V02 queue identity. Run PgFlow.Migration.up/0 first.'
      USING HINT = 'Apply core V02 before helpers V05.';
  END IF;
END
$pgflow_helpers_v05_prereq$;

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.start_tasks(text, bigint[], uuid);

--SPLIT--

-- Create "start_tasks" function
CREATE OR REPLACE FUNCTION $SCHEMA$."start_tasks" ("flow_slug" text, "msg_ids" bigint[], "worker_id" uuid, "queue_name" text) RETURNS SETOF $SCHEMA$."step_task_record" LANGUAGE sql SET "search_path" = '' AS $$
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
      -- Root steps (no dependencies) get empty object - they access flowInput via context.
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
  where _vr.visibility_updates >= 0
$$

--SPLIT--

-- WRITE: queue-aware stalled-task recovery on task snapshots (#650).
-- Keeps v04's dispatch predicate (started run + started step_state) and the
-- Elixir-side forcing/counting semantics from v02.
CREATE OR REPLACE FUNCTION $SCHEMA$.recover_stalled_tasks(p_stale_threshold double precision)
RETURNS TABLE(recovered_count bigint, vt_batches bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_requeued bigint;
  v_vt bigint;
  v_marked bigint;
  v_archived bigint;
  c_max_requeues constant int := 3;
BEGIN
  WITH stalled AS (
    SELECT st.run_id, st.step_slug, st.task_index, st.message_id, st.queue_name,
           st.requeued_count
    FROM pgflow.step_tasks st
    JOIN pgflow.runs  r ON r.run_id = st.run_id
    JOIN pgflow.flows f ON f.flow_slug = st.flow_slug
    JOIN pgflow.steps s ON s.flow_slug = st.flow_slug AND s.step_slug = st.step_slug
    JOIN pgflow.step_states ss
      ON ss.run_id = st.run_id AND ss.step_slug = st.step_slug
    WHERE st.status = 'started'
      AND st.permanently_stalled_at IS NULL
      AND r.status = 'started'
      AND ss.status = 'started'
      AND st.started_at < NOW()
          - (coalesce(s.opt_timeout, f.opt_timeout) * interval '1 second')
          - (p_stale_threshold * interval '1 second')
    FOR UPDATE OF st SKIP LOCKED
  ),
  to_requeue AS (SELECT * FROM stalled WHERE requeued_count < c_max_requeues),
  to_archive AS (SELECT * FROM stalled WHERE requeued_count >= c_max_requeues),
  requeued AS (
    UPDATE pgflow.step_tasks st
    SET status = 'queued',
        started_at = NULL,
        last_worker_id = NULL,
        requeued_count = st.requeued_count + 1,
        last_requeued_at = NOW()
    FROM to_requeue tr
    WHERE st.run_id = tr.run_id
      AND st.step_slug = tr.step_slug
      AND st.task_index = tr.task_index
    RETURNING tr.queue_name, tr.message_id
  ),
  visibility_reset AS (
    SELECT pgflow.set_vt_batch(r.queue_name, array_agg(r.message_id), array_agg(0::integer))
    FROM requeued r
    WHERE r.message_id IS NOT NULL
    GROUP BY r.queue_name
  ),
  mark_permanently_stalled AS (
    UPDATE pgflow.step_tasks st
    SET permanently_stalled_at = NOW()
    FROM to_archive ta
    WHERE st.run_id = ta.run_id
      AND st.step_slug = ta.step_slug
      AND st.task_index = ta.task_index
    RETURNING st.run_id
  ),
  archived AS (
    SELECT pgmq.archive(ta.queue_name, array_agg(ta.message_id))
    FROM to_archive ta
    WHERE ta.message_id IS NOT NULL
    GROUP BY ta.queue_name
  )
  SELECT
    (SELECT count(*) FROM requeued),
    (SELECT count(*) FROM visibility_reset),
    (SELECT count(*) FROM mark_permanently_stalled),
    (SELECT count(*) FROM archived)
  INTO v_requeued, v_vt, v_marked, v_archived;

  RETURN QUERY SELECT v_requeued, v_vt;
END;
$$;

--SPLIT--

-- WRITE: queue-aware pruning on task snapshots and persisted routes (#650).
-- Preserves the filtered Elixir API (interval + optional flow slug list).
DROP FUNCTION IF EXISTS $SCHEMA$.prune_data_older_than(interval, text[]);

--SPLIT--

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
  task_record RECORD;
  route_record RECORD;
  archive_table TEXT;
  dynamic_sql TEXT;
  filter_flows BOOLEAN := (p_flow_slugs IS NOT NULL AND array_length(p_flow_slugs, 1) > 0);
BEGIN
  DELETE FROM pgflow.workers
  WHERE last_heartbeat_at < cutoff_timestamp;
  GET DIAGNOSTICS v_deleted_workers = ROW_COUNT;

  FOR task_record IN
    SELECT
      st.queue_name,
      ARRAY_AGG(st.message_id) FILTER (WHERE st.message_id IS NOT NULL) AS message_ids
    FROM pgflow.runs r
    JOIN pgflow.step_tasks st ON st.run_id = r.run_id
    WHERE (
      (r.completed_at IS NOT NULL AND r.completed_at < cutoff_timestamp) OR
      (r.failed_at IS NOT NULL AND r.failed_at < cutoff_timestamp)
    )
    AND (NOT filter_flows OR r.flow_slug = ANY(p_flow_slugs))
    GROUP BY st.queue_name
  LOOP
    IF task_record.message_ids IS NOT NULL AND array_length(task_record.message_ids, 1) > 0 THEN
      PERFORM pgmq.delete(task_record.queue_name, task_record.message_ids);
    END IF;
  END LOOP;

  DELETE FROM pgflow.step_tasks
  WHERE run_id IN (
    SELECT run_id FROM pgflow.runs
    WHERE ((completed_at IS NOT NULL AND completed_at < cutoff_timestamp)
       OR (failed_at IS NOT NULL AND failed_at < cutoff_timestamp))
    AND (NOT filter_flows OR flow_slug = ANY(p_flow_slugs))
  );
  GET DIAGNOSTICS v_deleted_step_tasks = ROW_COUNT;

  DELETE FROM pgflow.step_states
  WHERE run_id IN (
    SELECT run_id FROM pgflow.runs
    WHERE ((completed_at IS NOT NULL AND completed_at < cutoff_timestamp)
       OR (failed_at IS NOT NULL AND failed_at < cutoff_timestamp))
    AND (NOT filter_flows OR flow_slug = ANY(p_flow_slugs))
  );
  GET DIAGNOSTICS v_deleted_step_states = ROW_COUNT;

  DELETE FROM pgflow.runs
  WHERE ((completed_at IS NOT NULL AND completed_at < cutoff_timestamp)
     OR (failed_at IS NOT NULL AND failed_at < cutoff_timestamp))
  AND (NOT filter_flows OR flow_slug = ANY(p_flow_slugs));
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
