defmodule Mix.Tasks.Pgflow.Gen.CronTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Pgflow.Gen.Cron, as: GenCron

  import ExUnit.CaptureIO

  @test_migrations_path "test/tmp/cron_migrations"

  setup do
    File.rm_rf!(@test_migrations_path)
    File.mkdir_p!(@test_migrations_path)

    on_exit(fn ->
      File.rm_rf!(@test_migrations_path)
    end)

    :ok
  end

  describe "run/1" do
    test "generates migration for a valid cron module" do
      output =
        capture_io(fn ->
          GenCron.run([
            "PgFlow.TestCrons.SimpleCron",
            "--migrations-path",
            @test_migrations_path
          ])
        end)

      assert output =~ "Generated migration:"
      assert output =~ "simple_cron"
      assert output =~ "mix ecto.migrate"
      assert output =~ "flow_type = 'cron'"
      assert output =~ "pg_cron"

      [migration_file] = File.ls!(@test_migrations_path)
      assert migration_file =~ "_compile_simple_cron.exs"

      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert migration_content =~ "defmodule PgFlow.Repo.Migrations.CompileSimpleCron"
      assert migration_content =~ "use Ecto.Migration"
      assert migration_content =~ "def up do"
      assert migration_content =~ "def down do"
      assert migration_content =~ "SELECT pgflow.create_flow('simple_cron'"
      assert migration_content =~ "SELECT pgflow.add_step('simple_cron', 'perform'"
      assert migration_content =~ "UPDATE pgflow.flows SET flow_type = 'cron'"
      assert migration_content =~ "cron.schedule('pgflow:simple_cron'"
      assert migration_content =~ "0 9 * * *"
    end

    test "generates migration for a cron with custom options and static input" do
      capture_io(fn ->
        GenCron.run([
          "PgFlow.TestCrons.DailyReportCron",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert migration_content =~ "create_flow('daily_report'"
      assert migration_content =~ "add_step('daily_report', 'perform'"
      assert migration_content =~ "flow_type = 'cron'"
      assert migration_content =~ "cron.schedule('pgflow:daily_report'"
      assert migration_content =~ "0 9 * * 1-5"
      assert migration_content =~ "report_type"
      assert migration_content =~ "daily"
    end

    test "down migration calls cron.unschedule before deleting flow data" do
      capture_io(fn ->
        GenCron.run([
          "PgFlow.TestCrons.SimpleCron",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      # Find positions of key statements in down migration
      unschedule_pos = :binary.match(migration_content, "cron.unschedule")
      delete_deps_pos = :binary.match(migration_content, "DELETE FROM pgflow.deps")
      delete_steps_pos = :binary.match(migration_content, "DELETE FROM pgflow.steps")
      delete_flows_pos = :binary.match(migration_content, "DELETE FROM pgflow.flows")
      drop_queue_pos = :binary.match(migration_content, "pgmq.drop_queue")

      # unschedule must come before all deletions
      assert elem(unschedule_pos, 0) < elem(delete_deps_pos, 0)
      assert elem(unschedule_pos, 0) < elem(delete_steps_pos, 0)
      assert elem(unschedule_pos, 0) < elem(delete_flows_pos, 0)
      assert elem(unschedule_pos, 0) < elem(drop_queue_pos, 0)

      assert migration_content =~ "cron.unschedule('pgflow:simple_cron')"
    end

    test "raises error when no module argument provided" do
      assert_raise Mix.Error, ~r/Missing cron module argument/, fn ->
        capture_io(fn ->
          GenCron.run(["--migrations-path", @test_migrations_path])
        end)
      end
    end

    test "raises error when module does not exist" do
      assert_raise Mix.Error, ~r/could not be loaded/, fn ->
        capture_io(fn ->
          GenCron.run([
            "NonExistent.CronModule",
            "--migrations-path",
            @test_migrations_path
          ])
        end)
      end
    end

    test "raises error when module is not a cron" do
      assert_raise Mix.Error, ~r/is not a PgFlow cron job/, fn ->
        capture_io(fn ->
          GenCron.run([
            "PgFlow.FlowCompiler",
            "--migrations-path",
            @test_migrations_path
          ])
        end)
      end
    end

    test "raises error when module is a job, not a cron" do
      assert_raise Mix.Error, ~r/is not a PgFlow cron job/, fn ->
        capture_io(fn ->
          GenCron.run([
            "PgFlow.TestJobs.SimpleJob",
            "--migrations-path",
            @test_migrations_path
          ])
        end)
      end
    end
  end

  describe "generated migration content" do
    test "includes proper module documentation" do
      capture_io(fn ->
        GenCron.run([
          "PgFlow.TestCrons.SimpleCron",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert migration_content =~ "@moduledoc"
      assert migration_content =~ "Compiles the 'simple_cron' cron job definition"
      assert migration_content =~ "Generated by: mix pgflow.gen.cron"
    end

    test "generates valid Elixir code" do
      capture_io(fn ->
        GenCron.run([
          "PgFlow.TestCrons.SimpleCron",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_path = Path.join(@test_migrations_path, migration_file)

      {result, _} = Code.eval_file(migration_path)
      assert match?({:module, PgFlow.Repo.Migrations.CompileSimpleCron, _, _}, result)
    end
  end
end
