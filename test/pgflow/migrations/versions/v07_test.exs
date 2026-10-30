defmodule PgFlow.Migrations.Versions.V07Test do
  @moduledoc false
  use ExUnit.Case, async: true

  alias PgFlow.Upstream.Claim

  @up_path "priv/pgflow_helpers/sql/versions/v07/v07_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v07/v07_down.sql"
  @statement_count 6

  test "v07 is the current helpers version" do
    assert PgFlow.HelpersMigration.current_version() == 7
  end

  test "verify_v03! accepts the recorded five-argument fragment" do
    assert Claim.verify_v03!() == :ok
  end

  test "up installs the recorded fragment once, as CREATE OR REPLACE" do
    up = File.read!(@up_path)
    fragment = Claim.eight_column_start_tasks_v03_sql()

    installed =
      String.replace_prefix(fragment, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")

    assert fragment =~ "$SCHEMA$"
    assert up =~ installed
    assert length(String.split(up, installed)) == 2
    assert length(String.split(up, "\n\n--SPLIT--\n\n")) == @statement_count
  end

  test "down fails forward-only" do
    down = File.read!(@down_path)
    assert down =~ "forward-only"
    refute down =~ "CREATE OR REPLACE FUNCTION"
  end
end
