defmodule PgFlow.Migrations.Versions.V05Test do
  @moduledoc false
  use ExUnit.Case, async: true

  alias PgFlow.Upstream.Claim

  @up_path "priv/pgflow_helpers/sql/versions/v05/v05_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v05/v05_down.sql"

  describe "registration" do
    test "v05 is the current helpers version" do
      assert PgFlow.HelpersMigration.current_version() == 5
    end
  end

  describe "claim reconciliation" do
    test "up embeds the recorded eight-column fragment" do
      up = File.read!(@up_path)
      assert up =~ Claim.eight_column_start_tasks_sql()
      assert Claim.verify!() == :ok
    end

    test "up drops the obsolete three-argument overload before installing the claim" do
      up = File.read!(@up_path)

      assert :binary.match(
               up,
               "DROP FUNCTION IF EXISTS $SCHEMA$.start_tasks(text, bigint[], uuid)"
             ) <
               :binary.match(up, "CREATE OR REPLACE FUNCTION $SCHEMA$.\"start_tasks\"")
    end
  end

  describe "queue-aware helpers" do
    test "up selects queue snapshots in recovery and pruning" do
      up = File.read!(@up_path)

      assert up =~ "SELECT st.run_id, st.step_slug, st.task_index, st.message_id, st.queue_name"
      assert up =~ "RETURNING tr.queue_name, tr.message_id"
      assert up =~ "pgmq.archive(ta.queue_name"
      assert up =~ "GROUP BY st.queue_name"
    end
  end

  describe "downgrade" do
    test "down fails forward-only" do
      assert File.read!(@down_path) =~ "forward-only"
      refute File.read!(@down_path) =~ "CREATE OR REPLACE FUNCTION"
    end
  end
end
