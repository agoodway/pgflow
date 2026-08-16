-- WRITE: Only recover tasks that `start_tasks` could actually hand out again.
--
-- v02 deadlined correctly but decided eligibility from the task row alone
-- (plus `runs.status <> 'failed'`). That is too loose once a step can end
-- without terminalizing its tasks.
--
-- `pgflow.fail_task` with `when_exhausted` in ('skip', 'skip-cascade') sets the
-- STEP to 'skipped' and archives its siblings' pgmq messages, but it never
-- touches the sibling `step_tasks` rows — they stay 'queued'/'started' forever
-- (same for `_cascade_force_skip_steps`). A map step that skips on its first
-- exhausted task therefore strands every started sibling in 'started' on a run
-- that goes on to COMPLETE. v02 saw those orphans past their deadline and
-- requeued them: status flipped back to 'queued' on a skipped step, three times
-- over, then `permanently_stalled_at` and an archive of an already-archived
-- message. The requeue is pure churn — `set_vt_batch` cannot resurrect an
-- archived message, so nothing is ever redelivered — but it corrupts the task
-- rows of a terminal run and makes the recovery counter lie.
--
-- The fix is to mirror `pgflow.start_tasks`'s own dispatch predicate: a task is
-- only worth reclaiming when its run is 'started' AND its step_state is
-- 'started'. Anything else (run completed/failed, step skipped/failed/
-- completed) can never be dispatched again, so requeuing it is never right.
-- This subsumes v02's `r.status <> 'failed'` guard.
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
    JOIN pgflow.step_states ss
      ON ss.run_id = st.run_id AND ss.step_slug = st.step_slug
    WHERE st.status = 'started'
      AND st.permanently_stalled_at IS NULL
      -- Dispatchable again? `start_tasks` requires both of these.
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
