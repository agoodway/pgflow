-- PgFlowDashboard Version 1 - Down Migration
-- Drops all objects created by v01_up.sql in reverse order

-- Drop query functions first (they depend on views)
DROP FUNCTION IF EXISTS $SCHEMA$.get_run_history_grid(text, integer);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_flow_steps(text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_job(text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_jobs();

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_crons();

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_cron(text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_flow(text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_flows();

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_adjacent_worker(uuid, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_worker_tasks(uuid, integer);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_worker(uuid);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_workers(text, text, integer, uuid);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.count_workers(text, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_step_tasks(uuid, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.list_step_states(uuid);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_adjacent_run(uuid, text);

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.get_run(uuid);

--SPLIT--

-- count_runs now has p_flow_type parameter
DROP FUNCTION IF EXISTS $SCHEMA$.count_runs(timestamptz, text, text, text);

--SPLIT--

-- list_runs now has p_flow_type parameter
DROP FUNCTION IF EXISTS $SCHEMA$.list_runs(timestamptz, text, text, integer, uuid, text);

--SPLIT--

-- Drop utility functions
DROP FUNCTION IF EXISTS $SCHEMA$.get_overview_metrics();

--SPLIT--

-- Drop views in dependency order (step_states_with_tasks may reference others)
DROP VIEW IF EXISTS $SCHEMA$.step_states_with_tasks;

--SPLIT--

DROP VIEW IF EXISTS $SCHEMA$.flow_stats;

--SPLIT--

DROP VIEW IF EXISTS $SCHEMA$.workers_with_load;

--SPLIT--

DROP VIEW IF EXISTS $SCHEMA$.runs_with_progress;

--SPLIT--

-- Finally drop the schema
DROP SCHEMA IF EXISTS $SCHEMA$;
