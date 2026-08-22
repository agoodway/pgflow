-- Reverse awaiting-signals helpers: drop park/signal/consume/expire, drop
-- task_signals, restore valid_status without `waiting`.

DROP FUNCTION IF EXISTS $SCHEMA$.park_waiting_task(uuid, text, integer, timestamptz)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.signal_task(uuid, text, integer, jsonb)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.consume_task_signal(uuid, text, integer)

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.expire_waiting_tasks()

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
