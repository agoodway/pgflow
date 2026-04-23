# Only start the test repository if we can connect
db_available? =
  case System.cmd("pg_isready", ["-h", "localhost", "-p", "54323"], stderr_to_stdout: true) do
    {_, 0} -> true
    _ -> false
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

  # pgmq is pre-installed in the atlas-postgres-pgflow image as an extension;
  # register it in the test DB once. On a bare Postgres image, replace this
  # with the SQL-only pgmq install migration.
  # (pg_cron is NOT registered — the atlas image's `cron.database_name` is
  # `postgres`, not our test DB. Tests that need pg_cron are tagged
  # separately and skipped when unavailable.)
  {:ok, _} = PgFlow.TestRepo.query("CREATE EXTENSION IF NOT EXISTS pgmq")

  # Run pgflow migrations BEFORE creating the realtime stub — if our
  # vendored SQL accidentally acquires a migration-time dependency on
  # `realtime.*`, we want the migration to fail loudly rather than
  # silently succeed because of the stub.
  Ecto.Migrator.up(PgFlow.TestRepo, 0, PgFlow.Test.CoreMigration, log: false)
  Ecto.Migrator.up(PgFlow.TestRepo, 1, PgFlow.Test.HelpersMigration, log: false)

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
