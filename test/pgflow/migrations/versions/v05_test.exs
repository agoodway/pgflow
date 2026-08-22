defmodule PgFlow.Migrations.Versions.V05Test do
  @moduledoc """
  Contract checks for the v05 helpers migration that do not need a database.

  Same rationale as `V04Test`: the DB-backed migration suite is tagged
  `:migration` and skipped wherever Postgres is absent, so "authored a version
  and forgot to register it" and "the `down` does not actually reverse the `up`"
  are asserted against the files here.
  """
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_helpers/sql/versions/v05/v05_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v05/v05_down.sql"

  defp up_sql, do: File.read!(@up_path)
  defp down_sql, do: File.read!(@down_path)

  describe "registration" do
    test "v05 is the current helpers version" do
      assert PgFlow.HelpersMigration.current_version() == 5
    end
  end

  describe "up SQL defines required objects" do
    test "widens valid_status and creates task_signals + functions" do
      up = up_sql()
      assert up =~ "waiting"
      assert up =~ "task_signals"
      assert up =~ "park_waiting_task"
      assert up =~ "signal_task"
      assert up =~ "consume_task_signal"
      assert up =~ "expire_waiting_tasks"
      assert File.exists?(@down_path)
    end
  end

  describe "down SQL reverses those objects" do
    test "drops functions and task_signals and restores valid_status without waiting" do
      down = down_sql()
      assert down =~ "park_waiting_task"
      assert down =~ "signal_task"
      assert down =~ "consume_task_signal"
      assert down =~ "expire_waiting_tasks"
      assert down =~ "DROP TABLE"
      assert down =~ "task_signals"
      assert down =~ "valid_status"

      refute down =~ ~r/ARRAY\[(?:'[^']+'::text,\s*)*'waiting'::text/
    end
  end
end
