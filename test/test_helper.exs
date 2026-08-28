# Only start the test repository if we can connect
db_available? =
  case System.cmd("pg_isready", ["-h", "localhost", "-p", "54323"], stderr_to_stdout: true) do
    {_, 0} -> true
    _ -> false
  end

# Set PGFLOW_REQUIRE_DB=1 in CI (or any run that must not silently skip) so an
# unreachable database aborts instead of quietly excluding every `:integration`
# test — an otherwise-green run that exercised none of the worker.
if not db_available? and System.get_env("PGFLOW_REQUIRE_DB") == "1" do
  raise """
  PGFLOW_REQUIRE_DB=1 is set but no database answered at localhost:54323.

  Start it with `docker compose up -d`, then re-run the suite.
  """
end

if db_available? do
  {:ok, _} = Application.ensure_all_started(:ecto_sql)
  {:ok, _} = PgFlow.TestRepo.start_link()

  # Bootstrap the schema exactly the way consumers do: via EctoEvolver-backed
  # PgFlow.Migration + PgFlow.HelpersMigration. This proves our generated
  # SQL bundle works against a clean Postgres + pgmq environment every test
  # run.
  defmodule PgFlow.Test.CoreMigration do
    use Ecto.Migration
    def up, do: PgFlow.Migration.up()
    def down, do: PgFlow.Migration.down()
  end

  defmodule PgFlow.Test.HelpersMigration do
    use Ecto.Migration
    def up, do: PgFlow.HelpersMigration.up()
    def down, do: PgFlow.HelpersMigration.down()
  end

  # pgmq and pg_cron are pre-installed in the atlas-postgres-pgflow image;
  # register them in the test DB once. The test compose configuration binds
  # pg_cron's metadata to pgflow_test through cron.database_name.
  {:ok, _} = PgFlow.TestRepo.query("CREATE EXTENSION IF NOT EXISTS pgmq")
  {:ok, _} = PgFlow.TestRepo.query("CREATE EXTENSION IF NOT EXISTS pg_cron")

  # Run pgflow migrations BEFORE creating the realtime stub — if our
  # vendored SQL accidentally acquires a migration-time dependency on
  # `realtime.*`, we want the migration to fail loudly rather than
  # silently succeed because of the stub.
  Ecto.Migrator.up(PgFlow.TestRepo, 0, PgFlow.Test.CoreMigration, log: false)
  Ecto.Migrator.up(PgFlow.TestRepo, 1, PgFlow.Test.HelpersMigration, log: false)

  # `Ecto.Migrator` skips a migration whose version it has already recorded, so
  # the line above is a no-op on any test DB created before a new helpers
  # version was authored — EctoEvolver never gets asked to apply it, and the
  # whole suite silently runs against the OLD helper function bodies. That
  # surfaces as an inexplicable failure in the new version's tests, so compare
  # what the database actually has against what the code defines and say so.
  applied_helpers_version =
    EctoEvolver.Adapters.Postgres.get_version(
      PgFlow.TestRepo,
      "pgflow",
      {:view, "extensions_version"}
    )

  if applied_helpers_version != PgFlow.HelpersMigration.current_version() do
    raise """
    Test database is at pgflow helpers version #{applied_helpers_version}, but \
    this checkout defines version #{PgFlow.HelpersMigration.current_version()}.

    Ecto.Migrator will not re-run the helpers migration against an existing
    database, so recreate it:

        MIX_ENV=test mix ecto.drop && MIX_ENV=test mix ecto.create
    """
  end

  # Stub `realtime.messages` + `realtime.send()` — the atlas image's
  # pre-baked pgflow.sql used to supply Supabase's realtime schema. Our
  # vendored pgflow SQL intentionally doesn't require it at migration time,
  # but plpgsql function bodies call `realtime.send(...)` at runtime and
  # `test_helpers.sql` uses realtime.messages for mocking + cleanup.
  {:ok, _} = PgFlow.TestRepo.query("CREATE SCHEMA IF NOT EXISTS realtime")

  {:ok, _} =
    PgFlow.TestRepo.query("""
    CREATE TABLE IF NOT EXISTS realtime.messages (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      topic text,
      event text,
      payload jsonb,
      private boolean DEFAULT false,
      inserted_at timestamptz DEFAULT now()
    )
    """)

  {:ok, _} =
    PgFlow.TestRepo.query("""
    CREATE OR REPLACE FUNCTION realtime.send(
      payload jsonb, event text, topic text, private boolean DEFAULT false
    ) RETURNS void AS $$
      INSERT INTO realtime.messages (topic, event, payload, private)
      VALUES (topic, event, payload, private);
    $$ LANGUAGE sql
    """)

  # Load pgflow_tests helper functions (`reset_db`, `read_and_start`, etc.)
  # These aren't part of the library's vendored SQL — they exist solely to
  # support the internal test suite. psql is used because the helpers
  # include multi-statement function bodies that Postgrex can't execute in
  # a single `query/1` call.
  {_, 0} =
    System.cmd(
      "psql",
      [
        "-h",
        "localhost",
        "-p",
        "54323",
        "-U",
        "postgres",
        "-d",
        "pgflow_test",
        "-v",
        "ON_ERROR_STOP=1",
        "-q",
        "-f",
        "test/support/db/test_helpers.sql"
      ],
      env: [{"PGPASSWORD", "postgres"}],
      stderr_to_stdout: true
    )

  Ecto.Adapters.SQL.Sandbox.mode(PgFlow.TestRepo, :manual)
  IO.puts("Database available - running all tests including integration")
  # `:migration` tests DROP/CREATE the pgflow schema; they pollute shared
  # state and must be run in isolation with `mix test --only migration`.
  ExUnit.start(exclude: [:migration])
else
  IO.puts("Database not available - skipping integration tests")
  ExUnit.start(exclude: [:integration, :migration])
end
