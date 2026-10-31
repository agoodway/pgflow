DO $pgflow_forward_only$
BEGIN
  RAISE EXCEPTION 'PgFlow helpers V07 is forward-only: the five-argument claim and four-argument register_worker cannot safely be discarded automatically. Restore from a pre-upgrade backup or roll forward with a corrective migration.';
END
$pgflow_forward_only$
