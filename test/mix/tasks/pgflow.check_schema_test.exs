defmodule Mix.Tasks.Pgflow.CheckSchemaTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mix.Tasks.Pgflow.CheckSchema
  alias PgFlow.SchemaCheck
  alias PgFlow.TestRepo

  import ExUnit.CaptureIO

  @moduletag :integration
  @moduletag :destructive_schema

  setup do
    Sandbox.mode(TestRepo, :auto)

    on_exit(fn ->
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  describe "SchemaCheck.run/1" do
    test "passes on the fully upgraded test database" do
      assert :ok = SchemaCheck.run(TestRepo)
    end

    @tag :destructive_schema
    test "rejects databases with the obsolete three-argument claim" do
      assert {:error, errors} =
               with_schema_change(
                 "DROP FUNCTION IF EXISTS pgflow.start_tasks(text, bigint[], uuid, text)"
               )

      assert Enum.any?(errors, &String.contains?(&1, "four-argument claim"))
    end

    @tag :destructive_schema
    test "rejects partial schemas missing queue_name constraints" do
      assert {:error, errors} =
               with_schema_change(
                 "ALTER TABLE pgflow.step_tasks DROP CONSTRAINT IF EXISTS queue_name_is_valid"
               )

      assert Enum.any?(errors, &String.contains?(&1, "queue_name_is_valid"))
    end

    @tag :destructive_schema
    test "rejects partial schemas with the seven-field step_task_record" do
      assert {:error, errors} =
               with_schema_change(
                 "ALTER TYPE pgflow.step_task_record DROP ATTRIBUTE IF EXISTS attempts_count"
               )

      assert Enum.any?(errors, &String.contains?(&1, "step_task_record layout"))
    end
  end

  describe "Mix.Tasks.Pgflow.CheckSchema.run/1" do
    test "prints success when the schema is compatible" do
      Sandbox.checkout(TestRepo)
      Sandbox.mode(TestRepo, {:shared, self()})

      output =
        capture_io(fn ->
          CheckSchema.run(["--repo", "PgFlow.TestRepo"])
        end)

      assert output =~ "All checks passed"
    end

    @tag :destructive_schema
    test "raises when the schema is incompatible" do
      Sandbox.checkout(TestRepo)
      Sandbox.mode(TestRepo, {:shared, self()})

      TestRepo.query!("DROP FUNCTION IF EXISTS pgflow.ensure_flow_compiled(text, jsonb)")

      try do
        assert_raise Mix.Error, ~r/not compatible/, fn ->
          capture_io(fn ->
            CheckSchema.run(["--repo", "PgFlow.TestRepo"])
          end)
        end
      after
        restore_compile_signature!()
      end
    end
  end

  defp with_schema_change(sql) do
    {:error, result} =
      TestRepo.transaction(fn ->
        TestRepo.query!(sql)
        TestRepo.rollback(SchemaCheck.run(TestRepo))
      end)

    result
  end

  defp restore_compile_signature! do
    sql_path = Path.join(:code.priv_dir(:pgflow), "pgflow_core/sql/versions/v02/v02_up.sql")

    statement =
      sql_path
      |> File.read!()
      |> String.split("--SPLIT--")
      |> Enum.find(
        &String.contains?(&1, ~s(CREATE OR REPLACE FUNCTION "pgflow"."ensure_flow_compiled"))
      )

    TestRepo.query!(String.trim(statement))
  end
end
