defmodule PgFlow.Migrations.Versions.V04Test do
  @moduledoc """
  Contract checks for the v04 helpers migration that do not need a database.

  Same rationale as `V03Test`: the DB-backed migration suite is tagged
  `:migration` and skipped wherever Postgres is absent, so "authored a version
  and forgot to register it" and "the `down` does not actually reverse the `up`"
  are asserted against the files here.
  """
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_helpers/sql/versions/v04/v04_up.sql"
  @down_path "priv/pgflow_helpers/sql/versions/v04/v04_down.sql"

  defp up_sql, do: File.read!(@up_path)
  defp down_sql, do: File.read!(@down_path)

  describe "registration" do
    test "v04 is registered in the helpers migration chain" do
      assert PgFlow.HelpersMigration.current_version() >= 4
    end
  end

  describe "up/down are inverses" do
    test "both replace recover_stalled_tasks with the same signature" do
      pattern =
        ~r/CREATE OR REPLACE FUNCTION \$SCHEMA\$\.recover_stalled_tasks\(p_stale_threshold double precision\)/

      assert up_sql() =~ pattern
      assert down_sql() =~ pattern
    end

    test "up adds the step_states guard and down drops it" do
      assert body(up_sql()) =~ ~r/JOIN pgflow\.step_states ss/
      assert body(up_sql()) =~ ~r/ss\.status = 'started'/

      refute body(down_sql()) =~ ~r/step_states/
    end

    test "up requires a started run where down only excludes failed ones" do
      assert body(up_sql()) =~ ~r/r\.status = 'started'/
      refute body(up_sql()) =~ ~r/r\.status <> 'failed'/

      assert body(down_sql()) =~ ~r/r\.status <> 'failed'/
    end

    test "down restores v02's body verbatim" do
      v02 = File.read!("priv/pgflow_helpers/sql/versions/v02/v02_up.sql")

      assert body(down_sql()) == body(v02),
             "v04_down must roll back to exactly the v02 function body"
    end
  end

  # Everything from the CREATE onward — the leading comment block differs by
  # design (each file explains its own direction).
  defp body(sql) do
    case String.split(sql, "CREATE OR REPLACE FUNCTION", parts: 2) do
      [_comment, create] ->
        create

      _ ->
        flunk("""
        Expected the SQL file to contain a CREATE OR REPLACE FUNCTION statement,
        but none was found. First 200 bytes:

        #{String.slice(sql, 0, 200)}
        """)
    end
  end
end
