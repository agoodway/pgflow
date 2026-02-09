defmodule PgFlow.EnqueueTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.Worker.Server

  @moduletag timeout: 30_000
  @moduletag :integration

  defmodule EnqueueTestJob do
    use PgFlow.Job

    @job slug: :enqueue_test_job, max_attempts: 3

    perform do
      fn input, _ctx ->
        %{result: input["value"]}
      end
    end
  end

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    :persistent_term.put({PgFlow, :repo}, TestRepo)

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})
      Sandbox.mode(TestRepo, :manual)
    end)

    compile_job(EnqueueTestJob)
    :ok
  end

  defp compile_job(job_module) do
    definition = job_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    max_attempts = definition.opts[:max_attempts] || 3
    base_delay = definition.opts[:base_delay] || 1
    timeout = definition.opts[:timeout] || 30

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )

    for step <- definition.steps do
      step_slug = Atom.to_string(step.slug)
      deps = Enum.map(step.depends_on, &Atom.to_string/1)
      step_type = Atom.to_string(step.step_type)

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          step_slug,
          deps,
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          step_type
        ]
      )
    end

    # Set flow_type to 'job'
    TestRepo.query!(
      "UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = $1",
      [flow_slug]
    )

    flow_slug
  end

  describe "PgFlow.enqueue/2" do
    test "enqueues a job and returns run_id" do
      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 42})
      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end

    test "enqueued job creates a run in the database" do
      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 99})

      {:ok, run_id_binary} = Ecto.UUID.dump(run_id)

      result =
        TestRepo.query!("SELECT status, input FROM pgflow.runs WHERE run_id = $1::uuid", [
          run_id_binary
        ])

      assert result.num_rows == 1
      [row] = result.rows
      [status, input] = row
      assert status == "started"
      assert input == %{"value" => 99}
    end
  end

  describe "PgFlow.enqueue/3" do
    test "enqueues a job with options (reserved for future use)" do
      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 1}, [])
      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end
  end

  describe "end-to-end job processing" do
    test "worker processes an enqueued job to completion" do
      {:ok, task_sup} = Task.Supervisor.start_link()

      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 42})

      {:ok, worker_pid} =
        Server.start_link(%{
          flow_module: EnqueueTestJob,
          repo: TestRepo,
          task_supervisor: task_sup,
          max_concurrency: 10,
          batch_size: 10,
          poll_interval: 0,
          visibility_timeout: 5,
          max_poll_seconds: 2,
          poll_interval_ms: 100
        })

      Process.unlink(worker_pid)

      {:ok, run_id_binary} = Ecto.UUID.dump(run_id)

      # Poll for completion
      result =
        Enum.reduce_while(1..100, nil, fn _i, _acc ->
          Process.sleep(100)

          query_result =
            TestRepo.query!("SELECT status FROM pgflow.runs WHERE run_id = $1::uuid", [
              run_id_binary
            ])

          case query_result.rows do
            [["completed"]] -> {:halt, :completed}
            [["failed"]] -> {:halt, :failed}
            _ -> {:cont, nil}
          end
        end)

      Server.stop(worker_pid)
      Supervisor.stop(task_sup)

      assert result == :completed
    end
  end

  describe "PgFlow.enqueue/2 error paths" do
    test "returns error when repo is not configured" do
      # Save current state
      current_repo = :persistent_term.get({PgFlow, :repo}, nil)
      current_env = Application.get_env(:pgflow, :repo)

      # Clear repo config
      :persistent_term.erase({PgFlow, :repo})
      Application.delete_env(:pgflow, :repo)

      result = PgFlow.enqueue(EnqueueTestJob, %{"value" => 1})
      assert {:error, "Repo not configured"} = result

      # Restore
      if current_repo, do: :persistent_term.put({PgFlow, :repo}, current_repo)
      if current_env, do: Application.put_env(:pgflow, :repo, current_env)
    end

    test "returns error for flow not compiled in DB" do
      defmodule NotInDbJob do
        use PgFlow.Job

        @job slug: :not_in_db_job

        perform do
          fn input, _ctx -> input end
        end
      end

      result = PgFlow.enqueue(NotInDbJob, %{"value" => 1})
      assert {:error, _reason} = result
    end
  end

  describe "flow_type persistence" do
    test "job is stored with flow_type 'job' in the database" do
      result =
        TestRepo.query!("SELECT flow_type FROM pgflow.flows WHERE flow_slug = 'enqueue_test_job'")

      assert result.num_rows == 1
      [[flow_type]] = result.rows
      assert flow_type == "job"
    end
  end
end
