-- WRITE: Awaiting signals — park a started task as `waiting` until an
-- external payload arrives (or the wait deadline expires).
--
-- Adds `waiting` to step_tasks.valid_status, a `task_signals` store for
-- early-buffered JSON payloads and wait deadlines, and park / signal /
-- consume / expire functions. Stalled recovery is left selecting only
-- `st.status = 'started'`; waiting tasks are not reclaimed as stalled.

ALTER TABLE $SCHEMA$.step_tasks DROP CONSTRAINT valid_status

--SPLIT--

ALTER TABLE $SCHEMA$.step_tasks
  ADD CONSTRAINT valid_status CHECK (
    status = ANY (ARRAY[
      'queued'::text,
      'started'::text,
      'completed'::text,
      'failed'::text,
      'waiting'::text
    ])
  )

--SPLIT--

CREATE TABLE IF NOT EXISTS $SCHEMA$.task_signals (
  run_id uuid NOT NULL,
  step_slug text NOT NULL,
  task_index integer NOT NULL DEFAULT 0,
  payload jsonb NULL,
  wait_deadline_at timestamptz NULL,
  timed_out boolean NOT NULL DEFAULT false,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_slug, task_index)
)

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.park_waiting_task(
  p_run_id uuid,
  p_step_slug text,
  p_task_index integer,
  p_wait_deadline_at timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task pgflow.step_tasks%ROWTYPE;
BEGIN
  SELECT *
    INTO v_task
    FROM pgflow.step_tasks
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index
     AND status = 'started'
     FOR UPDATE;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  IF v_task.message_id IS NOT NULL THEN
    PERFORM pgmq.archive(v_task.flow_slug, v_task.message_id);
  END IF;

  UPDATE pgflow.step_tasks
     SET status = 'waiting',
         started_at = NULL,
         message_id = NULL,
         attempts_count = GREATEST(attempts_count - 1, 0)
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index;

  INSERT INTO pgflow.task_signals (run_id, step_slug, task_index, wait_deadline_at)
  VALUES (p_run_id, p_step_slug, p_task_index, p_wait_deadline_at)
  ON CONFLICT (run_id, step_slug, task_index) DO UPDATE
    SET wait_deadline_at = COALESCE(pgflow.task_signals.wait_deadline_at, EXCLUDED.wait_deadline_at),
        updated_at = now();
END;
$$

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.signal_task(
  p_run_id uuid,
  p_step_slug text,
  p_task_index integer,
  p_payload jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_task pgflow.step_tasks%ROWTYPE;
  v_has_task boolean := false;
  v_msg_id bigint;
BEGIN
  SELECT *
    INTO v_task
    FROM pgflow.step_tasks
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index
     FOR UPDATE;

  v_has_task := FOUND;

  INSERT INTO pgflow.task_signals (run_id, step_slug, task_index, payload, timed_out)
  VALUES (p_run_id, p_step_slug, p_task_index, p_payload, false)
  ON CONFLICT (run_id, step_slug, task_index) DO UPDATE
    SET payload = EXCLUDED.payload,
        timed_out = false,
        updated_at = now();

  IF NOT v_has_task THEN
    -- No step_tasks row yet: early buffer only.
    RETURN;
  END IF;

  IF v_task.status <> 'waiting' THEN
    RETURN;
  END IF;

  SELECT send
    INTO v_msg_id
    FROM pgmq.send(
      v_task.flow_slug,
      jsonb_build_object(
        'flow_slug', v_task.flow_slug,
        'run_id', p_run_id,
        'step_slug', p_step_slug,
        'task_index', p_task_index
      )
    );

  UPDATE pgflow.step_tasks
     SET status = 'queued',
         message_id = v_msg_id,
         queued_at = now()
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index;
END;
$$

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.consume_task_signal(
  p_run_id uuid,
  p_step_slug text,
  p_task_index integer
)
RETURNS TABLE(payload jsonb, timed_out boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_row pgflow.task_signals%ROWTYPE;
BEGIN
  DELETE FROM pgflow.task_signals ts
   WHERE ts.run_id = p_run_id
     AND ts.step_slug = p_step_slug
     AND ts.task_index = p_task_index
     AND (ts.timed_out = true OR ts.payload IS NOT NULL)
  RETURNING * INTO v_row;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  IF v_row.timed_out THEN
    RETURN QUERY SELECT NULL::jsonb, true;
  ELSE
    RETURN QUERY SELECT v_row.payload, false;
  END IF;
END;
$$

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.expire_waiting_tasks()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  v_count bigint := 0;
  r record;
  v_msg_id bigint;
BEGIN
  FOR r IN
    SELECT st.run_id, st.step_slug, st.task_index, st.flow_slug
      FROM pgflow.step_tasks st
      JOIN pgflow.task_signals ts
        ON ts.run_id = st.run_id
       AND ts.step_slug = st.step_slug
       AND ts.task_index = st.task_index
     WHERE st.status = 'waiting'
       AND ts.wait_deadline_at IS NOT NULL
       AND ts.wait_deadline_at < now()
     FOR UPDATE OF st SKIP LOCKED
  LOOP
    UPDATE pgflow.task_signals
       SET timed_out = true,
           updated_at = now()
     WHERE run_id = r.run_id
       AND step_slug = r.step_slug
       AND task_index = r.task_index;

    SELECT send
      INTO v_msg_id
      FROM pgmq.send(
        r.flow_slug,
        jsonb_build_object(
          'flow_slug', r.flow_slug,
          'run_id', r.run_id,
          'step_slug', r.step_slug,
          'task_index', r.task_index
        )
      );

    UPDATE pgflow.step_tasks
       SET status = 'queued',
           message_id = v_msg_id,
           queued_at = now()
     WHERE run_id = r.run_id
       AND step_slug = r.step_slug
       AND task_index = r.task_index;

    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;
