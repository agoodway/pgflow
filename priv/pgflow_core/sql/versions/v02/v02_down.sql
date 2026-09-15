DO $pgflow_forward_only$
BEGIN
  RAISE EXCEPTION 'PgFlow core V02 is forward-only: new statuses and queue identities cannot safely be discarded automatically. Restore from a pre-upgrade backup or roll forward with a corrective migration.';
END
$pgflow_forward_only$
