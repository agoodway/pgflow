-- WRITE: Step-aware stalled-task recovery (replaces the v01 helper body).
--
-- Mirrors upstream pgflow's `pgflow.requeue_stalled_tasks()` (requeue cap of 3,
-- then archive + permanently_stalled_at; FOR UPDATE SKIP LOCKED) but differs on
-- purpose for the Elixir/OTP port:
--
--   * Deadlines on coalesce(step.opt_timeout, flow.opt_timeout) — the task's
--     EFFECTIVE timeout — not the flow timeout alone. This matches how
--     pgflow.start_tasks sets each message's pgmq visibility timeout
--     (coalesce(step, flow) + 2) and what upstream's own docs describe
--     ("step timeout + buffer"); the upstream SQL only consults the flow
--     timeout, so it reclaims a healthy long step (e.g. `timeout: 120` under a
--     30s flow default) mid-flight.
--   * Forces the side-effecting CTEs by consuming all four counts in the final
--     SELECT ... INTO, so set_vt_batch/pgmq.archive still run in an only-archive
--     sweep. Upstream's `FROM requeued, _vr, _mps, _ar` cross-join skips them
--     when nothing is requeued.
--   * Skips tasks whose run has failed (r.status <> 'failed'): start_tasks
--     won't re-pick a failed run's task, so requeuing it would strand it in
--     'queued'. Upstream does not filter run status.
--   * SECURITY DEFINER (like upstream's stored function) and invoked from a
--     supervised OTP GenServer rather than pg_cron.
--
-- `p_stale_threshold` is the buffer in seconds added beyond the effective
-- timeout before a task is considered stalled.
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
    SELECT st.run_id, st.step_slug, st.task_index, st.message_id, st.flow_slug,
           st.requeued_count
    FROM pgflow.step_tasks st
    JOIN pgflow.runs  r ON r.run_id = st.run_id
    JOIN pgflow.flows f ON f.flow_slug = st.flow_slug
    JOIN pgflow.steps s ON s.flow_slug = st.flow_slug AND s.step_slug = st.step_slug
    WHERE st.status = 'started'
      AND st.permanently_stalled_at IS NULL
      AND r.status <> 'failed'
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
    RETURNING tr.flow_slug AS queue_name, tr.message_id
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
    SELECT pgmq.archive(ta.flow_slug, array_agg(ta.message_id))
    FROM to_archive ta
    WHERE ta.message_id IS NOT NULL
    GROUP BY ta.flow_slug
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
