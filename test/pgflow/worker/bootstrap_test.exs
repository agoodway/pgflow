defmodule PgFlow.Worker.BootstrapTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Flow.Shape
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.Test.UnavailableRepo, as: UnavailableBootstrapRepo
  alias PgFlow.TestRepo
  alias PgFlow.Worker.Bootstrap
  alias PgFlow.Worker.Server
  @moduletag timeout: 30_000
  @moduletag :integration
  @moduletag :destructive_schema

  defmodule BootstrapCompileFlow do
    use PgFlow.Flow

    @flow slug: :bootstrap_compile_flow, max_attempts: 3, timeout: 60

    step :process do
      fn input, _ctx -> %{value: input["value"]} end
    end
  end

  defmodule BootstrapJobFlow do
    use PgFlow.Job

    @job slug: :bootstrap_job_flow

    perform do
      fn input, _ctx -> %{done: input["done"]} end
    end
  end

  defmodule BootstrapCronFlow do
    use PgFlow.Flow

    @flow slug: :bootstrap_cron_flow, cron: [schedule: "0 * * * *"]

    step :tick do
      fn _input, _ctx -> :ok end
    end
  end

  defmodule BootstrapTimeoutFlow do
    use PgFlow.Flow

    @flow slug: :bootstrap_timeout_flow, max_attempts: 3, timeout: 60

    step :slow do
      fn _input, _ctx -> Process.sleep(5_000) end
    end
  end

  setup do
    :ok = Sandbox.checkout(TestRepo)
    :ok = Sandbox.mode(TestRepo, {:shared, self()})
    ensure_v02_compilation_schema!()
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    :persistent_term.put({PgFlow, :repo}, TestRepo)

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  describe "Bootstrap.prepare/2" do
    test "missing or malformed helpers metadata remains schema incompatible" do
      for sql <- [
            "COMMENT ON VIEW pgflow.extensions_version IS NULL",
            "COMMENT ON VIEW pgflow.extensions_version IS 'missing version metadata'",
            "DROP VIEW pgflow.extensions_version"
          ] do
        TestRepo.query!(sql)

        assert {:error, {:schema_incompatible, :helpers_version}} =
                 PgFlow.SchemaCheck.runtime_checks(TestRepo)
      end
    end

    test "preserves a database failure from the helpers version lookup" do
      TestRepo.query!("""
      CREATE FUNCTION pgflow_tests.obj_description(oid) RETURNS text LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'version lookup unavailable' USING ERRCODE = '08006'; END
      $$
      """)

      TestRepo.query!("SET LOCAL search_path = pgflow_tests, pg_catalog")

      assert {:error, %Postgrex.Error{postgres: %{code: :connection_failure}}} =
               PgFlow.SchemaCheck.runtime_checks(TestRepo)
    end

    test "runtime contract matches the pinned interoperability manifest" do
      target =
        File.read!("test/support/upstream/manifest.json")
        |> Jason.decode!()
        |> Map.fetch!("target")

      for key <- ["claim_signature", "compile_signature"] do
        assert %{rows: [[true]]} =
                 TestRepo.query!("SELECT to_regprocedure($1::text) IS NOT NULL", [target[key]])
      end

      assert %{rows: rows} =
               TestRepo.query!("""
               SELECT attname FROM pg_attribute
               WHERE attrelid = (SELECT typrelid FROM pg_type WHERE oid = 'pgflow.step_task_record'::regtype)
                 AND attnum > 0 AND NOT attisdropped ORDER BY attnum
               """)

      assert Enum.map(rows, &hd/1) == target["record_fields"]
      assert :ok = PgFlow.SchemaCheck.runtime_checks(TestRepo)
      assert {:ok, _} = Bootstrap.prepare(TestRepo, BootstrapCompileFlow.__pgflow_definition__())
    end

    test "rejects a record layout with incorrect field types" do
      TestRepo.query!(
        "ALTER TYPE pgflow.step_task_record ALTER ATTRIBUTE attempts_count TYPE bigint CASCADE"
      )

      assert {:error, {:schema_incompatible, :record_layout}} =
               Bootstrap.prepare(TestRepo, BootstrapCompileFlow.__pgflow_definition__())
    end

    test "rejects old helpers even when claim signature exists" do
      TestRepo.query!("COMMENT ON VIEW pgflow.extensions_version IS 'PgFlow helpers version=4'")

      assert {:error, {:schema_incompatible, :helpers_version}} =
               Bootstrap.prepare(TestRepo, BootstrapCompileFlow.__pgflow_definition__())
    end

    test "rejects the legacy claim overload" do
      TestRepo.query!(
        "CREATE FUNCTION pgflow.start_tasks(text,bigint[],uuid) RETURNS SETOF pgflow.step_task_record LANGUAGE SQL AS 'SELECT * FROM pgflow.start_tasks($1,$2,$3,lower($1))'"
      )

      assert {:error, {:schema_incompatible, :start_tasks_legacy}} =
               Bootstrap.prepare(TestRepo, BootstrapCompileFlow.__pgflow_definition__())
    end

    test "rejects missing four argument claim" do
      TestRepo.query!("DROP FUNCTION pgflow.start_tasks(text,bigint[],uuid,text)")

      assert {:error, {:schema_incompatible, :start_tasks}} =
               Bootstrap.prepare(TestRepo, BootstrapCompileFlow.__pgflow_definition__())

      task_supervisor = start_supervised!(Task.Supervisor)

      config = %{
        flow_module: BootstrapCompileFlow,
        repo: TestRepo,
        task_supervisor: task_supervisor
      }

      assert {:error, {:bootstrap_failed, {:schema_incompatible, :start_tasks}}} =
               GenServer.start(Server, config)
    end

    test "compiles a missing flow at worker startup" do
      definition = BootstrapCompileFlow.__pgflow_definition__()

      assert {:ok, false} = Flows.flow_exists?(TestRepo, "bootstrap_compile_flow")

      assert {:ok, %{queue_name: "bootstrap_compile_flow", compilation_status: "compiled"}} =
               Bootstrap.prepare(TestRepo, definition)

      assert {:ok, true} = Flows.flow_exists?(TestRepo, "bootstrap_compile_flow")

      %{rows: [[step_slug]]} =
        TestRepo.query!(
          "SELECT step_slug FROM pgflow.steps WHERE flow_slug = $1",
          ["bootstrap_compile_flow"]
        )

      assert step_slug == "process"
    end

    test "verifies a matching shape without changing runtime tuning" do
      definition = BootstrapCompileFlow.__pgflow_definition__()

      TestRepo.query!(
        "SELECT pgflow.create_flow($1, $2, $3, $4)",
        ["bootstrap_compile_flow", 3, 1, 60]
      )

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, ARRAY[]::text[], NULL, NULL, NULL, NULL, 'single')",
        ["bootstrap_compile_flow", "process"]
      )

      TestRepo.query!(
        "UPDATE pgflow.flows SET opt_timeout = 99 WHERE flow_slug = $1",
        ["bootstrap_compile_flow"]
      )

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, %{compilation_status: "verified"}} =
                   Bootstrap.prepare(TestRepo, definition)
        end)

      assert log =~ "persisted execution options"
      assert log =~ "explicit database migration"

      %{rows: [[timeout]]} =
        TestRepo.query!("SELECT opt_timeout FROM pgflow.flows WHERE flow_slug = $1", [
          "bootstrap_compile_flow"
        ])

      assert timeout == 99
    end

    test "returns flow_shape_mismatch in production without deleting runs" do
      definition = BootstrapCompileFlow.__pgflow_definition__()

      TestRepo.query!("SELECT pgflow.create_flow($1)", ["bootstrap_compile_flow"])
      TestRepo.query!("SELECT pgflow.add_step($1, $2)", ["bootstrap_compile_flow", "process"])

      TestRepo.query!("SELECT pgflow.add_step($1, $2, ARRAY['process']::text[])", [
        "bootstrap_compile_flow",
        "extra_step"
      ])

      run_id = start_flow_run("bootstrap_compile_flow", %{})

      set_production_mode!()

      assert {:error, {:flow_shape_mismatch, differences}} =
               Bootstrap.prepare(TestRepo, definition)

      assert is_list(differences)
      assert get_run_status(run_id) == "started"

      assert worker_function_count("elixir:PgFlow.Worker.BootstrapTest.BootstrapCompileFlow") == 0
    end

    test "recompiles locally and preserves job flow_type" do
      definition = BootstrapJobFlow.__pgflow_definition__()

      TestRepo.query!("SELECT pgflow.create_flow($1)", ["bootstrap_job_flow"])
      TestRepo.query!("SELECT pgflow.add_step($1, $2)", ["bootstrap_job_flow", "old_step"])

      TestRepo.query!("UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = $1", [
        "bootstrap_job_flow"
      ])

      set_local_mode!()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, %{compilation_status: "recompiled"}} =
                   Bootstrap.prepare(TestRepo, definition)
        end)

      assert log =~ "run history was deleted"

      %{rows: [[flow_type]]} =
        TestRepo.query!("SELECT flow_type FROM pgflow.flows WHERE flow_slug = $1", [
          "bootstrap_job_flow"
        ])

      assert flow_type == "job"

      %{rows: steps} =
        TestRepo.query!("SELECT step_slug FROM pgflow.steps WHERE flow_slug = $1", [
          "bootstrap_job_flow"
        ])

      assert steps == [["bootstrap_job_flow"]]
    end

    test "registers elixir worker with process start mode after bootstrap" do
      definition = BootstrapCompileFlow.__pgflow_definition__()
      function_name = "elixir:PgFlow.Worker.BootstrapTest.BootstrapCompileFlow"

      assert {:ok, _} = Bootstrap.prepare(TestRepo, definition)

      %{rows: [[start_mode]]} =
        TestRepo.query!(
          "SELECT start_mode FROM pgflow.worker_functions WHERE function_name = $1",
          [function_name]
        )

      assert start_mode == "process"
    end

    @tag :destructive_schema
    test "returns schema error when ensure_flow_compiled is missing" do
      definition = BootstrapCompileFlow.__pgflow_definition__()

      TestRepo.query!("DROP FUNCTION IF EXISTS pgflow.ensure_flow_compiled(text, jsonb)")

      try do
        assert {:error, {:schema_incompatible, :ensure_flow_compiled}} =
                 Bootstrap.prepare(TestRepo, definition)
      after
        restore_core_functions!()
        TestRepo.query!("SELECT pgflow_tests.reset_db()")
      end
    end

    test "returns error on connectivity failure" do
      start_supervised!(UnavailableBootstrapRepo)
      definition = BootstrapCompileFlow.__pgflow_definition__()

      assert {:error, %DBConnection.ConnectionError{}} =
               Bootstrap.prepare(UnavailableBootstrapRepo, definition)
    end
  end

  describe "Flows.ensure_flow_compiled/3" do
    test "returns compiled status for a missing flow" do
      shape = BootstrapCompileFlow.__pgflow_definition__() |> Shape.from_definition()

      assert {:ok, %{status: "compiled", differences: []}} =
               Flows.ensure_flow_compiled(TestRepo, "bootstrap_compile_flow", shape)
    end
  end

  describe "Flows.execution_options/2" do
    test "returns effective timeout and retry options per step" do
      TestRepo.query!("SELECT pgflow.create_flow($1, 3, 1, 60)", ["bootstrap_exec_opts"])
      TestRepo.query!("SELECT pgflow.add_step($1, $2)", ["bootstrap_exec_opts", "root"])

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, ARRAY['root']::text[], 7, 2, 15, NULL, 'single')",
        ["bootstrap_exec_opts", "child"]
      )

      assert {:ok, options} = Flows.execution_options(TestRepo, "bootstrap_exec_opts")

      assert options["root"] == %{timeout: 60, max_attempts: 3, base_delay: 1}
      assert options["child"] == %{timeout: 15, max_attempts: 7, base_delay: 2}
    end
  end

  describe "worker startup integration" do
    setup do
      {:ok, task_supervisor} = Task.Supervisor.start_link()

      on_exit(fn ->
        try do
          if Process.alive?(task_supervisor), do: Supervisor.stop(task_supervisor)
        catch
          :exit, _ -> :ok
        end
      end)

      %{task_supervisor: task_supervisor}
    end

    test "missing flow compiles and worker registers without manual migration", %{
      task_supervisor: task_supervisor
    } do
      worker_pid = start_worker(BootstrapCompileFlow, task_supervisor)
      state = Server.get_state(worker_pid)

      assert state.compilation_status == "compiled"
      assert {:ok, true} = Flows.flow_exists?(TestRepo, "bootstrap_compile_flow")

      Server.stop(worker_pid)
    end

    test "connectivity failure during bootstrap prevents worker registration and polling", %{
      task_supervisor: task_supervisor
    } do
      Process.flag(:trap_exit, true)
      start_supervised!(UnavailableBootstrapRepo)

      config = %{
        flow_module: BootstrapCompileFlow,
        repo: UnavailableBootstrapRepo,
        task_supervisor: task_supervisor,
        max_concurrency: 1,
        batch_size: 1,
        signal_strategy: :polling,
        min_poll_interval: 50,
        max_poll_interval: 50,
        notify_fallback_interval: 30_000,
        heartbeat_interval: 10_000
      }

      assert {:error, {:bootstrap_failed, %DBConnection.ConnectionError{}}} =
               Server.start_link(config)

      Process.flag(:trap_exit, false)
    end

    test "uses database timeout changed between batches", %{task_supervisor: task_supervisor} do
      definition = BootstrapTimeoutFlow.__pgflow_definition__()
      shape = Shape.from_definition(definition)
      {:ok, _} = Flows.ensure_flow_compiled(TestRepo, "bootstrap_timeout_flow", shape)

      TestRepo.query!(
        "UPDATE pgflow.flows SET opt_timeout = 1, opt_max_attempts = 1 WHERE flow_slug = $1",
        ["bootstrap_timeout_flow"]
      )

      TestRepo.query!(
        "UPDATE pgflow.steps SET opt_timeout = NULL, opt_max_attempts = NULL WHERE flow_slug = $1",
        ["bootstrap_timeout_flow"]
      )

      worker_pid = start_worker(BootstrapTimeoutFlow, task_supervisor)
      run_id = start_flow_run("bootstrap_timeout_flow", %{})

      assert wait_until(
               fn ->
                 task = get_task_details(run_id, "slow")

                 task && task.status == "failed" && task.attempts_count >= 1
               end,
               timeout_ms: 15_000
             ) == :ok

      Server.stop(worker_pid)
    end

    test "does not duplicate cron schedules on worker restart", %{
      task_supervisor: task_supervisor
    } do
      definition = BootstrapCronFlow.__pgflow_definition__()
      shape = Shape.from_definition(definition)
      {:ok, _} = Flows.ensure_flow_compiled(TestRepo, "bootstrap_cron_flow", shape)

      job_name = "pgflow:bootstrap_cron_flow"
      TestRepo.query!("SELECT cron.schedule($1, $2, 'SELECT 1')", [job_name, "0 * * * *"])

      assert cron_job_count(job_name) == 1

      worker_pid = start_worker(BootstrapCronFlow, task_supervisor)
      assert cron_job_count(job_name) == 1

      Server.stop(worker_pid)

      _worker_pid = start_worker(BootstrapCronFlow, task_supervisor)
      assert cron_job_count(job_name) == 1
    end
  end

  describe "WorkerQueries.track_worker_function/3" do
    test "calls upstream registration with start mode" do
      function_name = "bootstrap_track_worker_test"

      assert {:ok, nil} = WorkerQueries.track_worker_function(TestRepo, function_name, "process")

      %{rows: [[start_mode]]} =
        TestRepo.query!(
          "SELECT start_mode FROM pgflow.worker_functions WHERE function_name = $1",
          [function_name]
        )

      assert start_mode == "process"
    end
  end

  defp start_worker(flow_module, task_supervisor) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: 2,
      batch_size: 2,
      signal_strategy: :polling,
      min_poll_interval: 50,
      max_poll_interval: 50,
      notify_fallback_interval: 30_000,
      heartbeat_interval: 10_000
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  defp start_flow_run(flow_slug, input) do
    %{rows: [[result]]} =
      TestRepo.query!("SELECT pgflow.start_flow($1, cast($2 as text)::jsonb)", [
        flow_slug,
        Jason.encode!(input)
      ])

    case result do
      {run_id, _, _, _, _, _, _, _, _} -> Ecto.UUID.load!(run_id)
      _ -> raise "unexpected start_flow result"
    end
  end

  defp get_task_details(run_id, step_slug) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT status, attempts_count
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = $2
        ORDER BY task_index
        LIMIT 1
        """,
        [Ecto.UUID.dump!(run_id), step_slug]
      )

    case rows do
      [[status, attempts]] -> %{status: status, attempts_count: attempts}
      [] -> nil
    end
  end

  defp get_run_status(run_id) do
    %{rows: [[status]]} =
      TestRepo.query!("SELECT status FROM pgflow.runs WHERE run_id = $1", [
        Ecto.UUID.dump!(run_id)
      ])

    status
  end

  defp worker_function_count(function_name) do
    %{rows: [[count]]} =
      TestRepo.query!(
        "SELECT count(*) FROM pgflow.worker_functions WHERE function_name = $1",
        [function_name]
      )

    count
  end

  defp cron_job_count(job_name) do
    %{rows: [[count]]} =
      TestRepo.query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [job_name])

    count
  end

  defp ensure_v02_compilation_schema! do
    {:ok, %{rows: [[exists?]]}} =
      TestRepo.query(
        "SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL"
      )

    if exists? do
      :ok
    else
      apply_core_v02_sql!()
    end
  end

  defp apply_core_v02_sql! do
    sql_path = Path.join(:code.priv_dir(:pgflow), "pgflow_core/sql/versions/v02/v02_up.sql")

    sql_path
    |> File.read!()
    |> String.split("--SPLIT--")
    |> Enum.filter(fn chunk ->
      String.contains?(chunk, ~s(CREATE OR REPLACE FUNCTION "pgflow"."ensure_flow_compiled"))
    end)
    |> List.last()
    |> case do
      nil -> raise "ensure_flow_compiled SQL not found in #{sql_path}"
      statement -> TestRepo.query!(String.trim(statement))
    end
  end

  defp restore_core_functions! do
    ensure_v02_compilation_schema!()
  end

  defp set_production_mode! do
    TestRepo.query!("SET LOCAL app.settings.jwt_secret = 'production-secret'")
  end

  defp set_local_mode! do
    TestRepo.query!(
      "SET LOCAL app.settings.jwt_secret = 'super-secret-jwt-token-with-at-least-32-characters-long'"
    )
  end

  defp wait_until(fun, opts) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 5_000)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 50)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline, poll_interval_ms)
  end

  defp do_wait_until(fun, deadline, poll_interval_ms) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) > deadline do
        {:error, :timeout}
      else
        Process.sleep(poll_interval_ms)
        do_wait_until(fun, deadline, poll_interval_ms)
      end
    end
  end
end
