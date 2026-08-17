defmodule PgFlowDashboard.Migrations.Versions.V03Test do
  @moduledoc """
  Contract checks for the v03 dashboard migration that do not need a database.

  Same rationale as the helpers-chain version tests: the DB-backed dashboard
  migration suite is tagged `:migration` and skipped wherever Postgres is
  absent, so "authored a version and forgot to register it" and "the `down`
  does not actually reverse the `up`" are asserted against the files here.
  """
  use ExUnit.Case, async: true

  @up_path "priv/pgflow_dashboard/sql/versions/v03/v03_up.sql"
  @down_path "priv/pgflow_dashboard/sql/versions/v03/v03_down.sql"
  @v02_up_path "priv/pgflow_dashboard/sql/versions/v02/v02_up.sql"

  defp up_sql, do: File.read!(@up_path)
  defp down_sql, do: File.read!(@down_path)

  describe "registration" do
    test "v03 is the current dashboard version" do
      assert PgFlowDashboard.Migration.current_version() == 3
    end

    test "the v03 version module exists and is listed in the migration module" do
      assert Code.ensure_loaded?(PgFlowDashboard.Migrations.Versions.V03)
      assert function_exported?(PgFlowDashboard.Migrations.Versions.V03, :up, 1)
      assert function_exported?(PgFlowDashboard.Migrations.Versions.V03, :down, 1)

      assert File.read!("lib/pgflow_dashboard/migration.ex") =~
               "PgFlowDashboard.Migrations.Versions.V03"
    end
  end

  describe "up/down are inverses" do
    test "both replace runs_with_progress and nothing else" do
      assert replaced_objects(up_sql()) == ["runs_with_progress"]
      assert replaced_objects(down_sql()) == ["runs_with_progress"]
    end

    test "up bounds a failed run's duration by failed_at" do
      assert view_body(up_sql()) =~
               "EXTRACT(EPOCH FROM (COALESCE(r.completed_at, r.failed_at, NOW()) - r.started_at)) * 1000 AS duration_ms"

      refute view_body(up_sql()) =~ "COALESCE(r.completed_at, NOW())"
    end

    test "down restores v02's runs_with_progress body verbatim" do
      assert view_body(down_sql()) == view_body(File.read!(@v02_up_path)),
             "v03_down must roll back to exactly the v02 view definition"
    end

    test "up and down move the version-tracking comment in opposite directions" do
      assert up_sql() =~
               "COMMENT ON VIEW $SCHEMA$.runs_with_progress IS 'PgFlowDashboard version=3'"

      assert down_sql() =~
               "COMMENT ON VIEW $SCHEMA$.runs_with_progress IS 'PgFlowDashboard version=2'"
    end

    test "neither direction changes the view's column list" do
      assert view_columns(up_sql()) == view_columns(File.read!(@v02_up_path))
      assert view_columns(down_sql()) == view_columns(File.read!(@v02_up_path))
    end
  end

  # Names of objects targeted by a CREATE OR REPLACE in the file.
  defp replaced_objects(sql) do
    ~r/CREATE\s+OR\s+REPLACE\s+(?:VIEW|FUNCTION)\s+\$SCHEMA\$\.(\w+)/i
    |> Regex.scan(sql)
    |> Enum.map(fn [_, name] -> name end)
    |> Enum.uniq()
  end

  # The runs_with_progress statement, from `CREATE OR REPLACE VIEW` onward.
  # Leading comments differ by design (each file explains its own direction).
  defp view_body(sql) do
    sql
    |> String.split("--SPLIT--")
    |> Enum.find_value(fn statement ->
      case String.split(statement, "CREATE OR REPLACE VIEW $SCHEMA$.runs_with_progress", parts: 2) do
        [_comment, body] -> String.trim(body)
        _ -> nil
      end
    end)
    |> case do
      nil ->
        flunk("""
        Expected a `CREATE OR REPLACE VIEW $SCHEMA$.runs_with_progress` statement,
        but none was found. First 200 bytes:

        #{String.slice(sql, 0, 200)}
        """)

      body ->
        body
    end
  end

  # `AS` aliases of the view's top-level select list, in order.
  defp view_columns(sql) do
    body = view_body(sql)

    ~r/\bAS\s+(\w+),?\s*$/m
    |> Regex.scan(body)
    |> Enum.map(fn [_, name] -> name end)
  end
end
