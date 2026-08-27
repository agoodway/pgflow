-- Reverse awaiting-signals helpers: drop atomic await/signal/expiry and
-- compatibility leftovers, drop task_signals, restore valid_status without
-- `waiting`.

DO $$
BEGIN
  -- EXCLUSIVE conflicts with both row-writer ROW EXCLUSIVE locks and the ROW
  -- SHARE locks taken by SELECT FOR UPDATE, while ordinary SELECT remains
  -- available. PostgreSQL retains these locks until the migration transaction
  -- ends, so no signal/park writer can cross the preflight/teardown boundary.
  LOCK TABLE pgflow.runs IN EXCLUSIVE MODE;
  LOCK TABLE pgflow.step_states IN EXCLUSIVE MODE;
  LOCK TABLE pgflow.step_tasks IN EXCLUSIVE MODE;
  LOCK TABLE pgflow.task_signals IN EXCLUSIVE MODE;

  IF EXISTS (SELECT 1 FROM pgflow.task_signals)
     OR EXISTS (SELECT 1 FROM pgflow.step_tasks WHERE status = 'waiting') THEN
    RAISE EXCEPTION
      'cannot roll pgflow helpers V05 back to V04 while task signals or waiting tasks exist; resolve or cancel them first';
  END IF;
END;
$$;

--SPLIT--

DROP TRIGGER IF EXISTS cleanup_terminal_step_signals ON $SCHEMA$.step_states

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.cleanup_terminal_step_signals()

--SPLIT--

DROP TRIGGER IF EXISTS cleanup_terminal_run_signals ON $SCHEMA$.runs

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.cleanup_terminal_run_signals()

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.park_waiting_task(uuid, text, integer, timestamptz)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.await_task_signal(uuid, text, integer, integer, bigint, bigint, boolean)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.signal_task(uuid, text, integer, jsonb)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.consume_task_signal(uuid, text, integer)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.expire_waiting_tasks(integer)

--SPLIT--

DROP INDEX IF EXISTS $SCHEMA$.task_signals_unresolved_deadline_idx

--SPLIT--

DROP TABLE IF EXISTS $SCHEMA$.task_signals

--SPLIT--

ALTER TABLE $SCHEMA$.step_tasks DROP CONSTRAINT IF EXISTS valid_status

--SPLIT--

ALTER TABLE $SCHEMA$.step_tasks
  ADD CONSTRAINT valid_status CHECK (
    status = ANY (ARRAY[
      'queued'::text,
      'started'::text,
      'completed'::text,
      'failed'::text
    ])
  );
