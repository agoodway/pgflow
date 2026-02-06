ALTER TABLE pgflow.flows DROP COLUMN IF EXISTS flow_type;
--SPLIT--
DROP FUNCTION IF EXISTS $SCHEMA$.recover_stalled_tasks(double precision);
--SPLIT--
DROP FUNCTION IF EXISTS $SCHEMA$.mark_worker_stopped(uuid);
--SPLIT--
DROP FUNCTION IF EXISTS $SCHEMA$.register_worker(uuid, text, text);
--SPLIT--
DROP FUNCTION IF EXISTS $SCHEMA$.get_step_output(uuid, text);
--SPLIT--
DROP FUNCTION IF EXISTS $SCHEMA$.flow_exists(text);
--SPLIT--
DROP FUNCTION IF EXISTS $SCHEMA$.get_flow_input(uuid);
--SPLIT--
DROP VIEW IF EXISTS $SCHEMA$.extensions_version;
