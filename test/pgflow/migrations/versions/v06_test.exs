defmodule PgFlow.Migrations.Versions.V06Test do
  @moduledoc false
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_helpers/sql/versions/v06/v06_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v06/v06_down.sql"

  test "v06 remains in the helpers migration chain" do
    assert PgFlow.HelpersMigration.current_version() >= 6
  end

  test "migration preserves schema substitution and statement boundaries" do
    up = File.read!(@up_path)
    down = File.read!(@down_path)

    assert up =~ "CREATE OR REPLACE FUNCTION $SCHEMA$.escalate_permanently_stalled()"
    assert up =~ "CREATE OR REPLACE VIEW $SCHEMA$.step_progress AS"
    assert up =~ "--SPLIT--"
    assert down =~ "DROP VIEW IF EXISTS $SCHEMA$.step_progress"
    assert down =~ "DROP FUNCTION IF EXISTS $SCHEMA$.escalate_permanently_stalled()"
  end
end
