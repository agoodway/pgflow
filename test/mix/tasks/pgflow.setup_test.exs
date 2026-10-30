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
    test "generates a versioned core and helpers upgrade migration" do
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

      [migration_file] = File.ls!(@test_migrations_path)
      assert migration_file =~ ~r/^\d{14}_upgrade_pgflow_core_v03_helpers_v07\.exs$/

      content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert content =~
               "defmodule PgFlow.TestRepo.Migrations.UpgradePgflowCoreV03HelpersV07 do"

      core_index = content |> :binary.match("PgFlow.Migration.up()") |> elem(0)
      helpers_index = content |> :binary.match("PgFlow.HelpersMigration.up()") |> elem(0)
      assert core_index < helpers_index
      assert content =~ "mix pgflow.audit_slugs"
      assert content =~ "stop every worker"
    end

    test "leaves an existing unversioned upgrade wrapper untouched" do
      old_path = Path.join(@test_migrations_path, "20200101000000_upgrade_pgflow.exs")
      File.write!(old_path, "old wrapper\n")

      capture_io(fn ->
        Setup.run([
          "--upgrade",
          "--repo",
          "PgFlow.TestRepo",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      assert File.read!(old_path) == "old wrapper\n"

      files = File.ls!(@test_migrations_path)
      assert Enum.any?(files, &(&1 =~ ~r/^\d{14}_upgrade_pgflow_core_v03_helpers_v07\.exs$/))
      assert length(files) == 2
    end

    test "keeps the dashboard step on the versioned upgrade wrapper" do
      capture_io(fn ->
        Setup.run([
          "--upgrade",
          "--dashboard",
          "--repo",
          "PgFlow.TestRepo",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      assert migration_file =~ ~r/^\d{14}_upgrade_pgflow_core_v03_helpers_v07\.exs$/
      content = File.read!(Path.join(@test_migrations_path, migration_file))
      assert content =~ "PgFlowDashboard.Migration.up()"
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
