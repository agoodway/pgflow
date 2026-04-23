defmodule PgFlow.MigrationTest do
  @moduledoc """
  End-to-end tests for the core schema migration shipped via EctoEvolver.

  Requires a live Postgres on `localhost:54323` with pgmq already installed
  (the pre-baked `atlas-postgres-pgflow` test image provides this). These
  tests DROP and recreate the `pgflow` schema so they're tagged `:migration`
  and excluded from the default run.

  Run with: `mix test --only migration`
  """

  use ExUnit.Case, async: false

  @moduletag :migration

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo

  # Wrapper migrations so EctoEvolver runs inside an Ecto.Migrator context.
  defmodule UpMigration do
    @moduledoc false
    use Ecto.Migration
    def change, do: PgFlow.Migration.up()
  end

  defmodule DownMigration do
    @moduledoc false
    use Ecto.Migration
    def change, do: PgFlow.Migration.down()
  end

  # Versions are arbitrary — they just need to be unique per test run so
  # `Ecto.Migrator.up/4` doesn't short-circuit on "already applied".
  @up_version 1_000_000_000_001
  @down_version 1_000_000_000_002

  setup do
    # Switch repo to `:auto` mode for these tests — DDL against a real DB
    # needs cross-process connection sharing that the default `:manual`
    # sandbox blocks.
    Sandbox.mode(TestRepo, :auto)

    on_exit(fn ->
      Sandbox.mode(TestRepo, :manual)
    end)

    TestRepo.query!("DROP SCHEMA IF EXISTS pgflow CASCADE")

    TestRepo.query!(
      "DELETE FROM schema_migrations WHERE version IN ($1, $2)",
      [@up_version, @down_version]
    )

    :ok
  end

  defp run_up!, do: Ecto.Migrator.up(TestRepo, @up_version, UpMigration, log: false)
  defp run_down!, do: Ecto.Migrator.up(TestRepo, @down_version, DownMigration, log: false)

  # Restore pgflow schema + extensions so the rest of the test suite sees a
  # usable DB (test_helper.exs bootstrapped HelpersMigration at startup;
  # our tests DROP the schema, so we must rebuild both halves here).
  setup_all do
    on_exit(fn ->
      Sandbox.mode(TestRepo, :auto)
      TestRepo.query!("DROP SCHEMA IF EXISTS pgflow CASCADE")

      TestRepo.query!(
        "DELETE FROM schema_migrations WHERE version IN ($1, $2)",
        [@up_version, @down_version]
      )

      _ = Ecto.Migrator.up(TestRepo, @up_version, UpMigration, log: false)

      defmodule ExtsMigration do
        @moduledoc false
        use Ecto.Migration
        def change, do: PgFlow.HelpersMigration.up()
      end

      _ = Ecto.Migrator.up(TestRepo, @up_version + 1, ExtsMigration, log: false)

      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  describe "up/0" do
    test "creates the pgflow schema" do
      run_up!()

      {:ok, %{rows: rows}} =
        TestRepo.query(
          "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
          ["pgflow"]
        )

      assert rows == [["pgflow"]]
    end

    test "creates core tables" do
      run_up!()

      tables = list_tables("pgflow")

      expected = ~w(flows runs steps step_states step_tasks workers)

      for name <- expected do
        assert name in tables, "expected pgflow.#{name} table to exist; got #{inspect(tables)}"
      end
    end

    test "creates the pgflow_version tracking view with EctoEvolver comment" do
      run_up!()

      {:ok, %{rows: [[comment]]}} =
        TestRepo.query("""
        SELECT obj_description(('pgflow.pgflow_version')::regclass, 'pg_class')
        """)

      assert comment =~ ~r/version=1/, "expected version=1 comment, got #{inspect(comment)}"
    end

    test "creates pgflow functions" do
      run_up!()

      functions = list_functions("pgflow")

      # Core functions that survive from the earliest upstream migration
      # through the latest; used by the Elixir PgFlow bindings.
      expected_functions = ~w(start_flow complete_task poll_for_tasks fail_task)

      for name <- expected_functions do
        assert name in functions,
               "expected pgflow.#{name} function; all: #{inspect(functions)}"
      end
    end

    test "is idempotent on a second up" do
      run_up!()
      table_count_before = count_tables("pgflow")

      # Second run: EctoEvolver's comment-based tracking should skip SQL.
      run_up!()

      assert count_tables("pgflow") == table_count_before
    end
  end

  describe "down/0" do
    test "drops the pgflow schema" do
      run_up!()
      run_down!()

      {:ok, %{rows: rows}} =
        TestRepo.query(
          "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
          ["pgflow"]
        )

      assert rows == []
    end
  end

  describe "legacy install preflight" do
    defmodule PreflightMigration do
      @moduledoc false
      use Ecto.Migration
      def change, do: PgFlow.Migration.up()
    end

    @preflight_version 1_000_000_000_003

    setup do
      on_exit(fn ->
        TestRepo.query!(
          "DELETE FROM schema_migrations WHERE version = $1",
          [@preflight_version]
        )
      end)
    end

    test "raises with stamp guidance when pgflow tables exist without tracking comment" do
      TestRepo.query!("CREATE SCHEMA pgflow")
      TestRepo.query!("CREATE TABLE pgflow.flows (flow_slug text PRIMARY KEY)")

      error =
        assert_raise RuntimeError, fn ->
          Ecto.Migrator.up(TestRepo, @preflight_version, PreflightMigration, log: false)
        end

      message = Exception.message(error)
      assert message =~ "legacy install detected"
      assert message =~ "mix pgflow.stamp --prefix pgflow"
    end

    test "raises on partial legacy install (only some sentinel tables exist)" do
      TestRepo.query!("CREATE SCHEMA pgflow")
      TestRepo.query!("CREATE TABLE pgflow.steps (flow_slug text, step_slug text)")

      assert_raise RuntimeError, ~r/legacy install detected/, fn ->
        Ecto.Migrator.up(TestRepo, @preflight_version, PreflightMigration, log: false)
      end
    end

    test "does not raise on a truly empty prefix (fresh install path)" do
      # No pgflow schema → no sentinel tables → preflight is a no-op and V01 runs.
      run_up!()

      assert count_tables("pgflow") > 0
    end

    test "does not raise after a successful up (tracked version > 0)" do
      run_up!()

      # Second up should go through Evolver.up/1 and no-op, not re-trigger preflight error.
      run_up!()

      assert count_tables("pgflow") > 0
    end
  end

  describe "Supabase-specific content is handled" do
    test "no standalone `CREATE EXTENSION pg_net` in the bundle" do
      # We can't check DB state directly (the test image ships pg_net),
      # but we can verify the vendored SQL doesn't try to create it.
      sql = File.read!("priv/pgflow_core/sql/versions/v01/v01_up.sql")
      refute sql =~ ~r/CREATE EXTENSION IF NOT EXISTS "pg_net"/
    end

    test "no standalone `CREATE SCHEMA pgmq` or `CREATE EXTENSION pgmq` in the bundle" do
      sql = File.read!("priv/pgflow_core/sql/versions/v01/v01_up.sql")
      refute sql =~ ~r/CREATE SCHEMA IF NOT EXISTS "pgmq"/
      refute sql =~ ~r/CREATE EXTENSION IF NOT EXISTS "pgmq"/
    end
  end

  # ── helpers ─────────────────────────────────────────────────────────────

  defp list_tables(schema) do
    {:ok, %{rows: rows}} =
      TestRepo.query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [schema]
      )

    Enum.map(rows, fn [name] -> name end)
  end

  defp count_tables(schema), do: schema |> list_tables() |> length()

  defp list_functions(schema) do
    {:ok, %{rows: rows}} =
      TestRepo.query(
        """
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_schema = $1 AND routine_type = 'FUNCTION'
        ORDER BY routine_name
        """,
        [schema]
      )

    Enum.map(rows, fn [name] -> name end)
  end
end
