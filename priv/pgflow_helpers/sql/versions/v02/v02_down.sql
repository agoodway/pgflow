-- Roll back to the v01 helper body (flat stale_threshold, LANGUAGE sql,
-- SECURITY INVOKER) so a downgrade to v01 is faithful.
CREATE OR REPLACE FUNCTION $SCHEMA$.recover_stalled_tasks(p_stale_threshold double precision)
RETURNS TABLE(recovered_count bigint, vt_batches bigint)
LANGUAGE sql VOLATILE
SECURITY INVOKER
SET search_path = ''
AS $$
  WITH stalled AS (
    UPDATE pgflow.step_tasks
    SET status = 'queued', started_at = NULL, last_worker_id = NULL
    WHERE status = 'started'
      AND started_at < NOW() - make_interval(secs => p_stale_threshold)
    RETURNING flow_slug, message_id
  ),
  vt_reset AS MATERIALIZED (
    SELECT pgflow.set_vt_batch(
      s.flow_slug,
      array_agg(s.message_id),
      array_agg(0::integer)
    )
    FROM stalled s
    WHERE s.message_id IS NOT NULL
    GROUP BY s.flow_slug
  )
  SELECT
    (SELECT count(*) FROM stalled)::bigint AS recovered_count,
    (SELECT count(*) FROM vt_reset)::bigint AS vt_batches;
$$;
