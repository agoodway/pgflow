# Only start the test repository if we can connect
db_available? =
  case System.cmd("pg_isready", ["-h", "localhost", "-p", "54323"], stderr_to_stdout: true) do
    {_, 0} -> true
    _ -> false
  end

if db_available? do
  {:ok, _} = Application.ensure_all_started(:ecto_sql)
  {:ok, _} = PgFlow.TestRepo.start_link()

  # Install PostgreSQL extension functions via EctoEvolver's real migration path
  defmodule PgFlow.Test.ExtensionsMigration do
    use Ecto.Migration
    def up, do: PgFlow.ExtensionsMigration.up()
    def down, do: PgFlow.ExtensionsMigration.down()
  end

  Ecto.Migrator.up(PgFlow.TestRepo, 0, PgFlow.Test.ExtensionsMigration, log: false)

  Ecto.Adapters.SQL.Sandbox.mode(PgFlow.TestRepo, :manual)
  IO.puts("Database available - running all tests including integration")
  ExUnit.start()
else
  IO.puts("Database not available - skipping integration tests")
  ExUnit.start(exclude: [:integration])
end
