defmodule Mix.Tasks.Pgflow.SetupTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Pgflow.Setup

  import ExUnit.CaptureIO

  @test_migrations_path "test/tmp/setup_migrations"

  setup do
    File.rm_rf!(@test_migrations_path)
    File.mkdir_p!(@test_migrations_path)

    on_exit(fn ->
      File.rm_rf!(@test_migrations_path)
    end)

    :ok
  end

  describe "run/1 upgrade wrapper" do
    test "generates a new timestamped upgrade migration" do
      output =
        capture_io(fn ->
          Setup.run([
            "--upgrade",
            "--repo",
            "PgFlow.TestRepo",
            "--migrations-path",
            @test_migrations_path
          ])
        end)

      assert output =~ "Created migration:"
      assert output =~ "upgrade_pgflow.exs"

      [migration_file] = File.ls!(@test_migrations_path)
      assert migration_file =~ ~r/^\d{14}_upgrade_pgflow\.exs$/
    end

    test "calls core migration before helpers migration" do
      capture_io(fn ->
        Setup.run([
          "--upgrade",
          "--repo",
          "PgFlow.TestRepo",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      content = File.read!(Path.join(@test_migrations_path, migration_file))

      core_index = :binary.match(content, "PgFlow.Migration.up()") |> elem(0)
      helpers_index = :binary.match(content, "PgFlow.HelpersMigration.up()") |> elem(0)

      assert core_index < helpers_index
    end

    test "uses a forward-only down migration" do
      capture_io(fn ->
        Setup.run([
          "--upgrade",
          "--repo",
          "PgFlow.TestRepo",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert content =~ "forward-only"
      assert content =~ "PgFlow.Migration.up()"
      assert content =~ "PgFlow.HelpersMigration.up()"
      refute content =~ "PgFlow.HelpersMigration.down()"
      refute content =~ "PgFlow.Migration.down()"
    end

    test "documents that an already-applied setup wrapper will not rerun" do
      capture_io(fn ->
        Setup.run([
          "--upgrade",
          "--repo",
          "PgFlow.TestRepo",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert content =~ "generate a **new**"
      assert content =~ "already recorded"
    end
  end
end
