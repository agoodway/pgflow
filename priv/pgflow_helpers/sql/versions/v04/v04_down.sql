-- Roll back to the v02 helper body: same step-aware deadline, requeue cap and
-- locking, but eligibility decided without consulting `step_states` (and with
-- the looser `runs.status <> 'failed'` guard).
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
