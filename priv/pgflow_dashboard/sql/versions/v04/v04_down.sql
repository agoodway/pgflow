-- Restore the released V03 worker view definition.
CREATE OR REPLACE VIEW $SCHEMA$.workers_with_load AS
SELECT
  w.worker_id,
  w.queue_name AS flow_slug,
  COALESCE(f.flow_type, 'flow') AS flow_type,
  w.last_heartbeat_at,
  CASE
    WHEN w.last_heartbeat_at > NOW() - INTERVAL '30 seconds' THEN 'healthy'
    WHEN w.last_heartbeat_at > NOW() - INTERVAL '60 seconds' THEN 'stale'
    ELSE 'dead'
  END AS health_status,
  COALESCE(tasks.active_count, 0) AS active_tasks,
  COALESCE(tasks.completed_count, 0) AS completed_tasks_24h
FROM pgflow.workers w
LEFT JOIN pgflow.flows f ON f.flow_slug = w.queue_name
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) FILTER (WHERE st.status = 'started') AS active_count,
    COUNT(*) FILTER (
      WHERE st.status = 'completed'
      AND st.completed_at > NOW() - INTERVAL '24 hours'
    ) AS completed_count
  FROM pgflow.step_tasks st
  JOIN pgflow.step_states ss ON st.run_id = ss.run_id AND st.step_slug = ss.step_slug
  JOIN pgflow.runs r ON ss.run_id = r.run_id
  WHERE r.flow_slug = w.queue_name
) tasks ON true;
