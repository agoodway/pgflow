DO $pgflow_forward_only$
BEGIN
  RAISE EXCEPTION 'PgFlow helpers V05 is forward-only: the four-argument queue-aware claim and snapshot-based recovery/pruning cannot safely be discarded automatically. Restore from a pre-upgrade backup or roll forward with a corrective migration.';
END
$pgflow_forward_only$
