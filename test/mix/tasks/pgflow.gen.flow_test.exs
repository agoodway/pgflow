defmodule Mix.Tasks.Pgflow.Gen.FlowTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Pgflow.Gen.Flow, as: GenFlow

  import ExUnit.CaptureIO

  @test_migrations_path "test/tmp/migrations"

  setup do
    # Clean up any previous test migrations
    File.rm_rf!(@test_migrations_path)
    File.mkdir_p!(@test_migrations_path)

    on_exit(fn ->
      File.rm_rf!(@test_migrations_path)
    end)

    :ok
  end

  describe "run/1" do
    test "generates migration for a valid flow module" do
      output =
        capture_io(fn ->
          GenFlow.run([
            "PgFlow.TestFlows.SimpleFlow",
            "--migrations-path",
            @test_migrations_path
          ])
        end)

      assert output =~ "Generated migration:"
      assert output =~ "simple_flow"
      assert output =~ "mix ecto.migrate"

      # Verify the migration file was created
      [migration_file] = File.ls!(@test_migrations_path)
      assert migration_file =~ "_compile_simple_flow.exs"

      # Read and verify the migration content
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert migration_content =~ "defmodule PgFlow.Repo.Migrations.CompileSimpleFlow"
      assert migration_content =~ "use Ecto.Migration"
      assert migration_content =~ "def up do"
      assert migration_content =~ "def down do"
      assert migration_content =~ "SELECT pgflow.create_flow('simple_flow'"
      assert migration_content =~ "SELECT pgflow.add_step('simple_flow', 'process'"
      assert migration_content =~ "DELETE FROM pgflow.flows WHERE flow_slug = 'simple_flow'"
      assert migration_content =~ "SELECT pgmq.drop_queue('simple_flow')"
    end

    test "generates migration for a flow with multiple steps" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.LinearFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      # Verify all steps are included
      assert migration_content =~ "'step_a'"
      assert migration_content =~ "'step_b'"
      assert migration_content =~ "'step_c'"

      # Verify dependencies
      assert migration_content =~ "ARRAY['step_a']::text[]"
      assert migration_content =~ "ARRAY['step_b']::text[]"
    end

    test "generates migration for a flow with map steps" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.DependentMapFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      # Verify map step type
      assert migration_content =~ "'map'"
      # Verify the map step has a dependency
      assert migration_content =~ "ARRAY['generate_items']::text[]"
    end

    test "generates migration for a flow with parallel steps" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.ParallelFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      # Verify step_d has multiple dependencies
      assert migration_content =~ "ARRAY['step_b', 'step_c']::text[]"
    end

    test "raises error when no module argument provided" do
      assert_raise Mix.Error, ~r/Missing flow module argument/, fn ->
        capture_io(fn ->
          GenFlow.run(["--migrations-path", @test_migrations_path])
        end)
      end
    end

    test "raises error when module does not exist" do
      assert_raise Mix.Error, ~r/could not be loaded/, fn ->
        capture_io(fn ->
          GenFlow.run([
            "NonExistent.Module",
            "--migrations-path",
            @test_migrations_path
          ])
        end)
      end
    end

    test "raises error when module is not a flow" do
      assert_raise Mix.Error, ~r/is not a PgFlow flow/, fn ->
        capture_io(fn ->
          GenFlow.run([
            "PgFlow.FlowCompiler",
            "--migrations-path",
            @test_migrations_path
          ])
        end)
      end
    end

    test "uses default migrations path when not specified" do
      default_path = "priv/repo/migrations"

      # Clean up default path if exists
      File.rm_rf!(default_path)

      output =
        capture_io(fn ->
          GenFlow.run(["PgFlow.TestFlows.SimpleFlow"])
        end)

      assert output =~ "Generated migration:"

      # Verify migration was created in default path
      assert File.exists?(default_path)
      [migration_file] = File.ls!(default_path)
      assert migration_file =~ "_compile_simple_flow.exs"

      # Clean up
      File.rm_rf!(default_path)
    end
  end

  describe "generated migration content" do
    test "includes proper module documentation" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.SimpleFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      assert migration_content =~ "@moduledoc"
      assert migration_content =~ "Compiles the 'simple_flow' flow definition"
      assert migration_content =~ "Generated by: mix pgflow.gen.flow"
    end

    test "generates valid Elixir code" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.LinearFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_path = Path.join(@test_migrations_path, migration_file)

      # Verify the generated code is valid Elixir
      {result, _} = Code.eval_file(migration_path)
      assert match?({:module, PgFlow.Repo.Migrations.CompileLinearFlow, _, _}, result)
    end

    test "generates correct down migration" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.SimpleFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      [migration_file] = File.ls!(@test_migrations_path)
      migration_content = File.read!(Path.join(@test_migrations_path, migration_file))

      # Verify down migration deletes in correct order (deps, steps, flows, then queue)
      assert migration_content =~ ~s|DELETE FROM pgflow.deps WHERE flow_slug = 'simple_flow'|
      assert migration_content =~ ~s|DELETE FROM pgflow.steps WHERE flow_slug = 'simple_flow'|
      assert migration_content =~ ~s|DELETE FROM pgflow.flows WHERE flow_slug = 'simple_flow'|
      assert migration_content =~ ~s|SELECT pgmq.drop_queue('simple_flow')|
    end
  end

  describe "timestamp generation" do
    test "generates unique timestamps for multiple migrations" do
      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.SimpleFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      # Wait a second to ensure different timestamp
      Process.sleep(1000)

      capture_io(fn ->
        GenFlow.run([
          "PgFlow.TestFlows.LinearFlow",
          "--migrations-path",
          @test_migrations_path
        ])
      end)

      files = File.ls!(@test_migrations_path) |> Enum.sort()
      assert length(files) == 2

      [first_file, second_file] = files
      first_timestamp = String.slice(first_file, 0, 14)
      second_timestamp = String.slice(second_file, 0, 14)

      assert first_timestamp != second_timestamp
    end
  end
end
