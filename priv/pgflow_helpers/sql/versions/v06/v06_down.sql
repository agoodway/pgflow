DROP VIEW IF EXISTS $SCHEMA$.step_progress;

--SPLIT--

DROP FUNCTION IF EXISTS $SCHEMA$.escalate_permanently_stalled();
