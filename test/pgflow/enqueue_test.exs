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
    test "enqueues a job with empty options" do
      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 1}, [])
      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end

    test "enqueues a job with delayed visibility" do
      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 1}, delay_seconds: 20)

      {:ok, run_id_binary} = Ecto.UUID.dump(run_id)

      %{rows: [[%DateTime{} = visible_at]]} =
        TestRepo.query!(
          """
          SELECT queue.vt
          FROM pgflow.step_tasks AS task
          JOIN pgmq.q_enqueue_test_job AS queue ON queue.msg_id = task.message_id
          WHERE task.run_id = $1::uuid
          """,
          [run_id_binary]
        )

      assert DateTime.diff(visible_at, DateTime.utc_now(), :second) in 17..22
    end

    test "enqueues a job with an absolute scheduled_at timestamp" do
      scheduled_at = DateTime.add(DateTime.utc_now(), 20, :second)

      {:ok, run_id} = PgFlow.enqueue(EnqueueTestJob, %{"value" => 1}, scheduled_at: scheduled_at)

      assert_visible_in(run_id, 17..22)
    end
  end

  describe "PgFlow.enqueue_in/3" do
    test "enqueues a job that becomes visible later" do
      {:ok, run_id} = PgFlow.enqueue_in(EnqueueTestJob, %{"value" => 1}, 20)

      assert_visible_in(run_id, 17..22)
    end

    test "zero seconds enqueues a job for immediate visibility" do
      {:ok, run_id} = PgFlow.enqueue_in(EnqueueTestJob, %{"value" => 1}, 0)

      assert_visible_in(run_id, -1..1)
    end

    test "rejects invalid delay values" do
      assert {:error, :invalid_delay_seconds} =
               PgFlow.enqueue_in(EnqueueTestJob, %{"value" => 1}, -1)

      assert {:error, :invalid_delay_seconds} =
               PgFlow.enqueue(EnqueueTestJob, %{"value" => 1}, delay_seconds: -1)
    end
  end

  describe "PgFlow.enqueue_at/3" do
    test "enqueues a job that becomes visible at the requested time" do
      scheduled_at = DateTime.add(DateTime.utc_now(), 20, :second)

      {:ok, run_id} = PgFlow.enqueue_at(EnqueueTestJob, %{"value" => 1}, scheduled_at)

      assert_visible_in(run_id, 17..22)
    end

    test "past timestamps enqueue a job for immediate visibility" do
      scheduled_at = DateTime.add(DateTime.utc_now(), -20, :second)

      {:ok, run_id} = PgFlow.enqueue_at(EnqueueTestJob, %{"value" => 1}, scheduled_at)

      assert_visible_in(run_id, -1..1)
    end

    test "rejects invalid scheduled timestamps" do
      assert {:error, :invalid_scheduled_at} =
               PgFlow.enqueue_at(EnqueueTestJob, %{"value" => 1}, "tomorrow")

      assert {:error, :invalid_scheduled_at} =
               PgFlow.enqueue(EnqueueTestJob, %{"value" => 1}, scheduled_at: "tomorrow")
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

  defp assert_visible_in(run_id, range) do
    {:ok, run_id_binary} = Ecto.UUID.dump(run_id)

    %{rows: [[%DateTime{} = visible_at]]} =
      TestRepo.query!(
        """
        SELECT queue.vt
        FROM pgflow.step_tasks AS task
        JOIN pgmq.q_enqueue_test_job AS queue ON queue.msg_id = task.message_id
        WHERE task.run_id = $1::uuid
        """,
        [run_id_binary]
      )

    assert DateTime.diff(visible_at, DateTime.utc_now(), :second) in range
  end
end
