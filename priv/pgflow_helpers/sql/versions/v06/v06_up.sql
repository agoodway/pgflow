-- Terminalize permanently stalled tasks through the step's when_exhausted policy.
-- Recovery stamps and archives them first; this function runs in a separate
-- transaction so it can take locks in fail_task's run, step, task order.
CREATE OR REPLACE FUNCTION $SCHEMA$.escalate_permanently_stalled()
RETURNS integer
LANGUAGE plpgsql
SET search_path = ''
AS $$
DECLARE
  v_candidate record;
  v_run_status text;
  v_step_status text;
  v_task_status text;
  v_stalled_at timestamptz;
  v_max_attempts integer;
  v_count integer := 0;
BEGIN
  FOR v_candidate IN
    SELECT st.run_id, st.step_slug, st.task_index, st.flow_slug
    FROM $SCHEMA$.step_tasks st
    WHERE st.status = 'started' AND st.permanently_stalled_at IS NOT NULL
    ORDER BY st.run_id, st.step_slug, st.task_index
  LOOP
    SELECT r.status, ss.status INTO v_run_status, v_step_status
    FROM $SCHEMA$.runs r
    JOIN $SCHEMA$.step_states ss ON ss.run_id = r.run_id
    WHERE r.run_id = v_candidate.run_id AND ss.step_slug = v_candidate.step_slug
    FOR UPDATE OF r, ss;

    IF v_run_status = 'started' AND v_step_status = 'started' THEN
      SELECT st.status, st.permanently_stalled_at
      INTO v_task_status, v_stalled_at
      FROM $SCHEMA$.step_tasks st
      WHERE st.run_id = v_candidate.run_id
        AND st.step_slug = v_candidate.step_slug
        AND st.task_index = v_candidate.task_index
      FOR UPDATE;

      IF v_task_status = 'started' AND v_stalled_at IS NOT NULL THEN
        SELECT COALESCE(s.opt_max_attempts, f.opt_max_attempts)
        INTO v_max_attempts
        FROM $SCHEMA$.steps s
        JOIN $SCHEMA$.flows f ON f.flow_slug = s.flow_slug
        WHERE s.flow_slug = v_candidate.flow_slug
          AND s.step_slug = v_candidate.step_slug;

        UPDATE $SCHEMA$.step_tasks st
        SET attempts_count = GREATEST(st.attempts_count, v_max_attempts)
        WHERE st.run_id = v_candidate.run_id
          AND st.step_slug = v_candidate.step_slug
          AND st.task_index = v_candidate.task_index;

        PERFORM $SCHEMA$.fail_task(
          v_candidate.run_id,
          v_candidate.step_slug,
          v_candidate.task_index,
          'permanently stalled: worker did not report after 3 requeues'
        );
        v_count := v_count + 1;
      END IF;
    END IF;
  END LOOP;

  RETURN v_count;
END;
$$;

--SPLIT--

-- Stable SQL read model for host applications that need run-step progress.
CREATE OR REPLACE VIEW $SCHEMA$.step_progress AS
SELECT r.run_id,
       r.flow_slug,
       r.status AS run_status,
       r.input AS run_input,
       r.started_at AS run_started_at,
       r.completed_at AS run_completed_at,
       r.failed_at AS run_failed_at,
       ss.step_slug,
       ss.status AS step_status,
       ss.skip_reason,
       ss.output AS step_output,
       ss.started_at AS step_started_at,
       ss.completed_at AS step_completed_at,
       ss.failed_at AS step_failed_at,
       ss.skipped_at AS step_skipped_at
FROM $SCHEMA$.runs r
JOIN $SCHEMA$.step_states ss ON ss.run_id = r.run_id AND ss.flow_slug = r.flow_slug;
