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

  describe "repo discovery" do
    test "runs without --repo using the host app's configured repos" do
      output = capture_io(fn -> CheckSchema.run([]) end)

      assert output =~ "Checking pgflow schema in PgFlow.TestRepo"
    end
  end

  describe "SchemaCheck.run/1" do
    test "passes on the fully upgraded test database" do
      assert PgFlow.Migration.current_version() == 3
      assert PgFlow.HelpersMigration.current_version() == 7
      assert :ok = SchemaCheck.run(TestRepo)
      assert :ok = SchemaCheck.runtime_checks(TestRepo)
    end

    @tag :destructive_schema
    test "rejects a recreated four-argument claim" do
      assert {:error, {:schema_incompatible, :start_tasks_legacy_4arg}} =
               with_runtime_change("""
               CREATE FUNCTION pgflow.start_tasks(text, bigint[], uuid, text)
               RETURNS SETOF pgflow.step_task_record
               LANGUAGE sql AS $$
                 SELECT * FROM pgflow.start_tasks($1, $2, $3, $4, NULL::text)
               $$
               """)

      assert {:error, errors} =
               with_schema_change("""
               CREATE FUNCTION pgflow.start_tasks(text, bigint[], uuid, text)
               RETURNS SETOF pgflow.step_task_record
               LANGUAGE sql AS $$
                 SELECT * FROM pgflow.start_tasks($1, $2, $3, $4, NULL::text)
               $$
               """)

      assert Enum.any?(errors, &String.contains?(&1, "obsolete four-argument claim"))
    end

    @tag :destructive_schema
    test "rejects a recreated three-argument claim" do
      sql = """
      CREATE FUNCTION pgflow.start_tasks(text, bigint[], uuid)
      RETURNS SETOF pgflow.step_task_record
      LANGUAGE sql AS $$
        SELECT * FROM pgflow.start_tasks($1, $2, $3, lower($1), NULL::text)
      $$
      """

      assert {:error, {:schema_incompatible, :start_tasks_legacy_3arg}} = with_runtime_change(sql)
      assert {:error, errors} = with_schema_change(sql)
      assert Enum.any?(errors, &String.contains?(&1, "start_tasks(text,bigint[],uuid)"))
    end

    @tag :destructive_schema
    test "rejects helpers older than the bundled version" do
      assert {:error, {:schema_incompatible, :helpers_version}} =
               with_runtime_change(
                 "COMMENT ON VIEW pgflow.extensions_version IS 'PgFlow helpers version=6'"
               )
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

      TestRepo.query!(
        "DROP FUNCTION IF EXISTS pgflow.ensure_flow_compiled(text, jsonb, text, jsonb)"
      )

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

  defp with_runtime_change(sql) do
    {:error, result} =
      TestRepo.transaction(fn ->
        TestRepo.query!(sql)
        TestRepo.rollback(SchemaCheck.runtime_checks(TestRepo))
      end)

    result
  end

  defp restore_compile_signature! do
    sql_path = Path.join(:code.priv_dir(:pgflow), "pgflow_core/sql/versions/v03/v03_up.sql")

    statement =
      sql_path
      |> File.read!()
      |> String.split("--SPLIT--")
      |> Enum.find(&String.contains?(&1, ~s(CREATE FUNCTION "pgflow"."ensure_flow_compiled")))

    TestRepo.query!(String.trim(statement))
  end
end
