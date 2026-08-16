defmodule PgFlow.Migrations.Versions.V03Test do
  @moduledoc """
  Contract checks for the v03 helpers migration that do not need a database.

  The DB-backed migration suite is tagged `:migration` and is skipped wherever
  Postgres is absent, so the two mistakes most likely to ship unnoticed —
  authoring a version and forgetting to register it, and an `up` whose `down`
  does not actually reverse it — are asserted against the files here instead.
  """
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_helpers/sql/versions/v03/v03_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v03/v03_down.sql"

  defp up_sql, do: File.read!(@up_path)
  defp down_sql, do: File.read!(@down_path)

  describe "registration" do
    # EctoEvolver derives `current_version` from the length of the registered
    # list, so "at least 3" is what proves v03 is still in the chain. The
    # newest version pins the exact number — see V04Test.
    test "v03 is registered in the helpers migration chain" do
      assert PgFlow.HelpersMigration.current_version() >= 3
    end
  end

  describe "up/down are inverses" do
    test "up adds the attribute and down drops it" do
      assert up_sql() =~ ~r/ALTER TYPE \$SCHEMA\$\.step_task_record ADD ATTRIBUTE attempts_count/

      assert down_sql() =~
               ~r/ALTER TYPE \$SCHEMA\$\.step_task_record DROP ATTRIBUTE attempts_count/
    end

    test "both replace start_tasks, so the rollback restores a body that matches the type" do
      assert up_sql() =~ ~r/CREATE OR REPLACE FUNCTION \$SCHEMA\$\.start_tasks/
      assert down_sql() =~ ~r/CREATE OR REPLACE FUNCTION \$SCHEMA\$\.start_tasks/
    end

    test "down narrows the type only after the function stops selecting the attribute" do
      sql = down_sql()

      [replace_at, drop_at] = [
        :binary.match(sql, "CREATE OR REPLACE"),
        :binary.match(sql, "DROP ATTRIBUTE")
      ]

      assert elem(replace_at, 0) < elem(drop_at, 0),
             "dropping the attribute before replacing the function fails on the dependent function"
    end
  end

  describe "the returned attempt" do
    test "up selects the stored count plus one" do
      assert up_sql() =~ ~r/st\.attempts_count \+ 1 as attempts_count/
    end

    test "down returns a start_tasks that selects no attempts_count column" do
      refute down_sql() =~ ~r/as attempts_count/
    end
  end
end
