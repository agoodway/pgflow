-- WRITE: Awaiting signals — park a started task as `waiting` until an
-- external payload arrives (or the wait deadline expires).
--
-- Adds `waiting` to step_tasks.valid_status, a `task_signals` store for
-- early-buffered JSON payloads and wait deadlines, and atomic await / signal /
-- expire functions. Stalled recovery is left selecting only
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
  ) NOT VALID

-- Operator follow-up: run this in a later separately committed migration, not this V05 transaction:
-- ALTER TABLE pgflow.step_tasks VALIDATE CONSTRAINT valid_status;

--SPLIT--

CREATE TABLE IF NOT EXISTS $SCHEMA$.task_signals (
  run_id uuid NOT NULL,
  step_slug text NOT NULL,
  task_index integer NOT NULL DEFAULT 0,
  payload jsonb NULL,
  wait_deadline_at timestamptz NULL,
  timed_out boolean NOT NULL DEFAULT false,
  claimed_at timestamptz NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_slug, task_index),
  CONSTRAINT task_signals_task_index_nonnegative CHECK (task_index >= 0),
  CONSTRAINT task_signals_payload_shape CHECK (
    payload IS NULL OR jsonb_typeof(payload) IN ('object', 'array')
  ),
  CONSTRAINT task_signals_step_state_fkey
    FOREIGN KEY (run_id, step_slug)
    REFERENCES $SCHEMA$.step_states(run_id, step_slug)
    ON DELETE CASCADE
)

--SPLIT--

CREATE INDEX task_signals_unresolved_deadline_idx
ON $SCHEMA$.task_signals (wait_deadline_at)
WHERE wait_deadline_at IS NOT NULL
  AND timed_out = false
  AND payload IS NULL

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.signal_task(
  p_run_id uuid,
  p_step_slug text,
  p_task_index integer,
  p_payload jsonb
)
RETURNS TABLE(outcome text)
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_run pgflow.runs%ROWTYPE;
  v_step pgflow.step_states%ROWTYPE;
  v_task pgflow.step_tasks%ROWTYPE;
  v_signal pgflow.task_signals%ROWTYPE;
  v_has_task boolean := false;
  v_has_signal boolean := false;
  v_msg_id bigint;
BEGIN
  IF p_payload IS NULL
     OR jsonb_typeof(p_payload) NOT IN ('object', 'array') THEN
    RAISE EXCEPTION 'signal payload must be a JSON object or array'
      USING ERRCODE = '22023';
  END IF;

  IF pg_column_size(p_payload) > 1048576 THEN
    RAISE EXCEPTION 'signal payload exceeds the 1048576-byte database limit'
      USING ERRCODE = '22023';
  END IF;

  SELECT *
    INTO v_run
    FROM pgflow.runs
   WHERE run_id = p_run_id
     FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'missing'::text;
    RETURN;
  END IF;

  SELECT *
    INTO v_step
    FROM pgflow.step_states
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'missing'::text;
    RETURN;
  END IF;

  SELECT *
    INTO v_task
    FROM pgflow.step_tasks
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index
     FOR UPDATE;

  v_has_task := FOUND;

  SELECT *
    INTO v_signal
    FROM pgflow.task_signals
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index
     FOR UPDATE;

  v_has_signal := FOUND;

  IF v_run.status <> 'started'
     OR v_step.status NOT IN ('created', 'started')
     OR (v_has_task AND v_task.status IN ('completed', 'failed')) THEN
    RETURN QUERY SELECT 'terminal'::text;
    RETURN;
  END IF;

  IF v_has_signal
     AND v_signal.claimed_at IS NOT NULL
     AND NOT v_signal.timed_out THEN
    RETURN QUERY SELECT 'already_delivered'::text;
    RETURN;
  END IF;

  IF v_signal.timed_out
     OR (v_signal.wait_deadline_at IS NOT NULL AND v_signal.wait_deadline_at <= now()) THEN
    IF v_signal.payload IS NULL THEN
      UPDATE pgflow.task_signals
         SET timed_out = true,
             updated_at = now()
       WHERE run_id = p_run_id
         AND step_slug = p_step_slug
         AND task_index = p_task_index;

      IF v_has_task AND v_task.status = 'waiting' THEN
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
      END IF;
    END IF;

    RETURN QUERY SELECT 'expired'::text;
    RETURN;
  END IF;

  INSERT INTO pgflow.task_signals (run_id, step_slug, task_index, payload)
  VALUES (p_run_id, p_step_slug, p_task_index, p_payload)
  ON CONFLICT (run_id, step_slug, task_index) DO UPDATE
    SET payload = EXCLUDED.payload,
        updated_at = now();

  IF NOT v_has_task OR v_task.status IN ('queued', 'started') THEN
    RETURN QUERY SELECT 'buffered'::text;
    RETURN;
  END IF;

  IF v_task.status <> 'waiting' THEN
    RETURN QUERY SELECT 'missing'::text;
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

  RETURN QUERY SELECT 'requeued'::text;
END;
$$

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.await_task_signal(
  p_run_id uuid,
  p_step_slug text,
  p_task_index integer,
  p_expected_attempt integer,
  p_expected_message_id bigint,
  p_wait_for_seconds bigint,
  p_park boolean
)
RETURNS TABLE(outcome text, payload jsonb)
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_run pgflow.runs%ROWTYPE;
  v_step pgflow.step_states%ROWTYPE;
  v_task pgflow.step_tasks%ROWTYPE;
  v_signal pgflow.task_signals%ROWTYPE;
  v_has_task boolean := false;
  v_has_signal boolean := false;
  v_deadline timestamptz;
BEGIN
  SELECT *
    INTO v_run
    FROM pgflow.runs
   WHERE run_id = p_run_id
     FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'missing'::text, NULL::jsonb;
    RETURN;
  END IF;

  SELECT *
    INTO v_step
    FROM pgflow.step_states
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     FOR UPDATE;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'missing'::text, NULL::jsonb;
    RETURN;
  END IF;

  SELECT *
    INTO v_task
    FROM pgflow.step_tasks
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index
     FOR UPDATE;

  v_has_task := FOUND;

  SELECT *
    INTO v_signal
    FROM pgflow.task_signals
   WHERE run_id = p_run_id
     AND step_slug = p_step_slug
     AND task_index = p_task_index
     FOR UPDATE;

  v_has_signal := FOUND;

  IF v_run.status <> 'started'
     OR v_step.status <> 'started' THEN
    RETURN QUERY SELECT 'terminal'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF NOT v_has_task THEN
    RETURN QUERY SELECT 'missing'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF v_task.status <> 'started'
     OR v_task.attempts_count <> p_expected_attempt
     OR v_task.message_id IS DISTINCT FROM p_expected_message_id THEN
    RETURN QUERY SELECT 'stale'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF v_has_signal AND v_signal.timed_out THEN
    UPDATE pgflow.task_signals
    SET claimed_at = COALESCE(claimed_at, now()), updated_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
    RETURN QUERY SELECT 'timeout'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF v_has_signal AND v_signal.payload IS NOT NULL THEN
    UPDATE pgflow.task_signals
    SET claimed_at = COALESCE(claimed_at, now()), updated_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
    RETURN QUERY SELECT 'signal'::text, v_signal.payload;
    RETURN;
  END IF;

  IF v_has_signal AND v_signal.wait_deadline_at IS NOT NULL
                  AND v_signal.wait_deadline_at <= now() THEN
    UPDATE pgflow.task_signals
    SET timed_out = true,
        claimed_at = COALESCE(claimed_at, now()),
        updated_at = now()
    WHERE run_id = p_run_id AND step_slug = p_step_slug AND task_index = p_task_index;
    RETURN QUERY SELECT 'timeout'::text, NULL::jsonb;
    RETURN;
  END IF;

  IF NOT p_park THEN
    RETURN QUERY SELECT 'empty'::text, NULL::jsonb;
    RETURN;
  END IF;

  v_deadline := CASE
    WHEN p_wait_for_seconds IS NULL THEN NULL
    ELSE now() + make_interval(secs => p_wait_for_seconds)
  END;

  INSERT INTO pgflow.task_signals (run_id, step_slug, task_index, wait_deadline_at)
  VALUES (p_run_id, p_step_slug, p_task_index, v_deadline)
  ON CONFLICT (run_id, step_slug, task_index) DO UPDATE
    SET wait_deadline_at = COALESCE(pgflow.task_signals.wait_deadline_at, EXCLUDED.wait_deadline_at),
        updated_at = now();

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
    AND task_index = p_task_index
    AND status = 'started'
    AND attempts_count = p_expected_attempt
    AND message_id IS NOT DISTINCT FROM p_expected_message_id;

  IF NOT FOUND THEN
    RETURN QUERY SELECT 'stale'::text, NULL::jsonb;
  ELSE
    RETURN QUERY SELECT 'parked'::text, NULL::jsonb;
  END IF;
END;
$$

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.expire_waiting_tasks(p_limit integer)
RETURNS bigint
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_count bigint := 0;
  candidate_row record;
  v_run pgflow.runs%ROWTYPE;
  v_step pgflow.step_states%ROWTYPE;
  v_task pgflow.step_tasks%ROWTYPE;
  v_signal pgflow.task_signals%ROWTYPE;
  v_msg_id bigint;
  v_updated bigint;
BEGIN
  -- Coordinate with V05 rollback before candidate discovery touches any table.
  -- ROW SHARE conflicts with rollback's EXCLUSIVE mode but remains compatible
  -- with ordinary ROW EXCLUSIVE DML. PostgreSQL retains all four locks through
  -- this transaction, so rollback either owns runs first or waits behind the
  -- complete canonical lock set.
  LOCK TABLE pgflow.runs IN ROW SHARE MODE;
  LOCK TABLE pgflow.step_states IN ROW SHARE MODE;
  LOCK TABLE pgflow.step_tasks IN ROW SHARE MODE;
  LOCK TABLE pgflow.task_signals IN ROW SHARE MODE;

  IF p_limit IS NULL OR p_limit <= 0 THEN
    RETURN 0;
  END IF;

  FOR candidate_row IN
    SELECT candidate.run_id,
           candidate.step_slug,
           candidate.task_index,
           candidate.wait_deadline_at
      FROM pgflow.task_signals candidate
     WHERE candidate.wait_deadline_at IS NOT NULL
       AND candidate.wait_deadline_at <= now()
       AND candidate.timed_out = false
       AND candidate.payload IS NULL
     ORDER BY candidate.wait_deadline_at,
              candidate.run_id,
              candidate.step_slug,
              candidate.task_index
     LIMIT p_limit
  LOOP
    SELECT *
      INTO v_run
      FROM pgflow.runs r
     WHERE r.run_id = candidate_row.run_id
       FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
      CONTINUE;
    END IF;

    SELECT *
      INTO v_step
      FROM pgflow.step_states ss
     WHERE ss.run_id = candidate_row.run_id
       AND ss.step_slug = candidate_row.step_slug
       FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
      CONTINUE;
    END IF;

    SELECT *
      INTO v_task
      FROM pgflow.step_tasks st
     WHERE st.run_id = candidate_row.run_id
       AND st.step_slug = candidate_row.step_slug
       AND st.task_index = candidate_row.task_index
       FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
      CONTINUE;
    END IF;

    SELECT *
      INTO v_signal
      FROM pgflow.task_signals ts
     WHERE ts.run_id = candidate_row.run_id
       AND ts.step_slug = candidate_row.step_slug
       AND ts.task_index = candidate_row.task_index
       FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
      CONTINUE;
    END IF;

    IF v_run.status <> 'started'
       OR v_step.status <> 'started'
       OR v_task.status <> 'waiting'
       OR v_signal.timed_out
       OR v_signal.payload IS NOT NULL
       OR v_signal.wait_deadline_at IS NULL
       OR v_signal.wait_deadline_at > now() THEN
      CONTINUE;
    END IF;

    UPDATE pgflow.task_signals ts
       SET timed_out = true,
           updated_at = now()
     WHERE ts.run_id = candidate_row.run_id
       AND ts.step_slug = candidate_row.step_slug
       AND ts.task_index = candidate_row.task_index
       AND ts.timed_out = false
       AND ts.payload IS NULL
       AND ts.wait_deadline_at IS NOT NULL
       AND ts.wait_deadline_at <= now();

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    IF v_updated <> 1 THEN
      CONTINUE;
    END IF;

    SELECT send
      INTO v_msg_id
      FROM pgmq.send(
        v_task.flow_slug,
        jsonb_build_object(
          'flow_slug', v_task.flow_slug,
          'run_id', candidate_row.run_id,
          'step_slug', candidate_row.step_slug,
          'task_index', candidate_row.task_index
        )
      );

    UPDATE pgflow.step_tasks
       SET status = 'queued',
           message_id = v_msg_id,
           queued_at = now()
     WHERE run_id = candidate_row.run_id
       AND step_slug = candidate_row.step_slug
       AND task_index = candidate_row.task_index
       AND status = 'waiting';

    v_count := v_count + 1;
  END LOOP;

  RETURN v_count;
END;
$$;

--SPLIT--

REVOKE EXECUTE ON FUNCTION $SCHEMA$.await_task_signal(uuid, text, integer, integer, bigint, bigint, boolean) FROM PUBLIC

--SPLIT--

REVOKE EXECUTE ON FUNCTION $SCHEMA$.signal_task(uuid, text, integer, jsonb) FROM PUBLIC

--SPLIT--

REVOKE EXECUTE ON FUNCTION $SCHEMA$.expire_waiting_tasks(integer) FROM PUBLIC

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_step_signals()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
BEGIN
  IF NEW.status IN ('completed', 'failed', 'skipped')
     AND OLD.status IS DISTINCT FROM NEW.status THEN
    UPDATE pgflow.step_tasks
       SET status = 'failed',
           failed_at = COALESCE(failed_at, now()),
           error_message = COALESCE(error_message, 'abandoned: step became ' || NEW.status),
           message_id = NULL
     WHERE run_id = NEW.run_id
       AND step_slug = NEW.step_slug
       AND status = 'waiting';

    DELETE FROM pgflow.task_signals
     WHERE run_id = NEW.run_id
       AND step_slug = NEW.step_slug;
  END IF;

  RETURN NEW;
END;
$$;

--SPLIT--

CREATE TRIGGER cleanup_terminal_step_signals
AFTER UPDATE OF status ON $SCHEMA$.step_states
FOR EACH ROW EXECUTE FUNCTION $SCHEMA$.cleanup_terminal_step_signals();

--SPLIT--

REVOKE EXECUTE ON FUNCTION $SCHEMA$.cleanup_terminal_step_signals() FROM PUBLIC

--SPLIT--

CREATE OR REPLACE FUNCTION $SCHEMA$.cleanup_terminal_run_signals()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $$
DECLARE
  v_message_ids bigint[];
BEGIN
  IF NEW.status = 'failed'
     AND OLD.status IS DISTINCT FROM NEW.status THEN
    UPDATE pgflow.step_states
       SET status = 'failed',
           failed_at = COALESCE(failed_at, now()),
           error_message = COALESCE(error_message, 'abandoned: run became failed')
     WHERE run_id = NEW.run_id
       AND status IN ('created', 'started');

    PERFORM 1
      FROM pgflow.step_tasks
     WHERE run_id = NEW.run_id
       AND status IN ('queued', 'started', 'waiting')
     ORDER BY step_slug, task_index
       FOR UPDATE;

    SELECT array_agg(message_id ORDER BY step_slug, task_index)
      INTO v_message_ids
      FROM pgflow.step_tasks
     WHERE run_id = NEW.run_id
       AND status IN ('queued', 'started', 'waiting')
       AND message_id IS NOT NULL;

    UPDATE pgflow.step_tasks
       SET status = 'failed',
           failed_at = COALESCE(failed_at, now()),
           error_message = COALESCE(error_message, 'abandoned: run became failed'),
           message_id = NULL
     WHERE run_id = NEW.run_id
       AND status IN ('queued', 'started', 'waiting');

    IF v_message_ids IS NOT NULL THEN
      PERFORM pgmq.archive(NEW.flow_slug, v_message_ids);
    END IF;

    DELETE FROM pgflow.task_signals
     WHERE run_id = NEW.run_id;
  ELSIF NEW.status = 'completed'
        AND OLD.status IS DISTINCT FROM NEW.status THEN
    UPDATE pgflow.step_tasks
       SET status = 'failed',
           failed_at = COALESCE(failed_at, now()),
           error_message = COALESCE(error_message, 'abandoned: run became ' || NEW.status),
           message_id = NULL
     WHERE run_id = NEW.run_id
       AND status = 'waiting';

    DELETE FROM pgflow.task_signals
     WHERE run_id = NEW.run_id;
  END IF;

  RETURN NEW;
END;
$$;

--SPLIT--

CREATE TRIGGER cleanup_terminal_run_signals
AFTER UPDATE OF status ON $SCHEMA$.runs
FOR EACH ROW EXECUTE FUNCTION $SCHEMA$.cleanup_terminal_run_signals();

--SPLIT--

REVOKE EXECUTE ON FUNCTION $SCHEMA$.cleanup_terminal_run_signals() FROM PUBLIC
