defmodule PgFlow.WorkersTest do
  use PgFlow.IntegrationCase

  alias PgFlow.Schema.StepTask
  alias PgFlow.{Workers, WorkerSummary}

  defmodule UnavailableRepo do
    def exists?(_query) do
      raise DBConnection.ConnectionError, message: "database unavailable"
    end
  end

  describe "healthy?/2" do
    test "is true only when the requested flow has a fresh active worker" do
      create_flow("ready_workers")
      create_flow("other_workers")
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", "ready_workers", seconds_ago: 5)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2", "other_workers", seconds_ago: 5)

      assert {:ok, true} = Workers.healthy?(TestRepo, "ready_workers")
      assert {:ok, true} = Workers.healthy?(TestRepo, "other_workers")
      assert {:ok, false} = Workers.healthy?(TestRepo, "missing_workers")
    end

    test "is false for stale, stopped, and deprecated workers" do
      create_flow("stale_workers")
      create_flow("stopped_workers")
      create_flow("deprecated_workers")

      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3", "stale_workers", seconds_ago: 31)
      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4", "stopped_workers", stopped: true)

      insert_worker("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa5", "deprecated_workers",
        deprecated: true
      )

      assert {:ok, false} = Workers.healthy?(TestRepo, "stale_workers")
      assert {:ok, false} = Workers.healthy?(TestRepo, "stopped_workers")
      assert {:ok, false} = Workers.healthy?(TestRepo, "deprecated_workers")
    end

    test "returns repository connection failures as error tuples" do
      assert {:error, %DBConnection.ConnectionError{}} =
               Workers.healthy?(UnavailableRepo, "ready_workers")
    end
  end

  describe "get/2, list/2, and count/2" do
    test "return typed summaries with health and bounded flow-load calculations" do
      create_flow("worker_reads")
      add_step("worker_reads", "work")
      create_flow("idle_worker_reads")

      healthy_id = "ffffffff-ffff-ffff-ffff-fffffffffff1"
      stale_id = "ffffffff-ffff-ffff-ffff-fffffffffff2"
      dead_id = "ffffffff-ffff-ffff-ffff-fffffffffff3"
      idle_id = "ffffffff-ffff-ffff-ffff-fffffffffff4"

      insert_worker(healthy_id, "worker_reads", seconds_ago: 5)
      insert_worker(stale_id, "worker_reads", seconds_ago: 45)
      insert_worker(dead_id, "worker_reads", seconds_ago: 90)
      insert_worker(idle_id, "idle_worker_reads", seconds_ago: 5)

      active_run_id = start_flow_run("worker_reads", %{"kind" => "active"})
      completed_run_id = start_flow_run("worker_reads", %{"kind" => "completed"})
      old_completed_run_id = start_flow_run("worker_reads", %{"kind" => "old"})

      update_task(active_run_id, "started", healthy_id)
      update_task(completed_run_id, "completed", healthy_id, hours_ago: 1)
      update_task(old_completed_run_id, "completed", healthy_id, hours_ago: 25)

      assert {:ok,
              %WorkerSummary{
                worker_id: ^healthy_id,
                flow_slug: "worker_reads",
                flow_type: "flow",
                health_status: "healthy",
                active_tasks: 1,
                completed_tasks_24h: 1
              }} = Workers.get(TestRepo, healthy_id)

      assert {:ok,
              [
                %WorkerSummary{
                  worker_id: ^stale_id,
                  health_status: "stale",
                  active_tasks: 1,
                  completed_tasks_24h: 1
                }
              ]} =
               Workers.list(TestRepo, flow_slug: "worker_reads", health_status: :stale)

      assert {:ok,
              [
                %WorkerSummary{
                  worker_id: ^dead_id,
                  health_status: "dead",
                  active_tasks: 1,
                  completed_tasks_24h: 1
                }
              ]} =
               Workers.list(TestRepo, flow_slug: "worker_reads", health_status: "dead")

      assert {:ok,
              [
                %WorkerSummary{
                  worker_id: ^idle_id,
                  active_tasks: 0,
                  completed_tasks_24h: 0
                }
              ]} = Workers.list(TestRepo, flow_slug: "idle_worker_reads")

      assert {:ok, 3} = Workers.count(TestRepo, flow_slug: "worker_reads")

      assert {:ok, 1} =
               Workers.count(TestRepo, flow_slug: "worker_reads", health_status: :healthy)
    end

    test "classifies stopped and deprecated workers as dead despite fresh heartbeats" do
      create_flow("terminal_worker_reads")

      stopped_id = "ffffffff-ffff-ffff-ffff-ffffffffffe1"
      deprecated_id = "ffffffff-ffff-ffff-ffff-ffffffffffe2"

      insert_worker(stopped_id, "terminal_worker_reads", stopped: true)
      insert_worker(deprecated_id, "terminal_worker_reads", deprecated: true)

      assert {:ok, %WorkerSummary{health_status: "dead"}} =
               Workers.get(TestRepo, stopped_id)

      assert {:ok, %WorkerSummary{health_status: "dead"}} =
               Workers.get(TestRepo, deprecated_id)

      assert {:ok, []} =
               Workers.list(TestRepo,
                 flow_slug: "terminal_worker_reads",
                 health_status: :healthy
               )

      assert {:ok, terminal_workers} =
               Workers.list(TestRepo,
                 flow_slug: "terminal_worker_reads",
                 health_status: :dead
               )

      assert Enum.map(terminal_workers, & &1.worker_id) |> Enum.sort() ==
               Enum.sort([stopped_id, deprecated_id])

      assert {:ok, 2} =
               Workers.count(TestRepo,
                 flow_slug: "terminal_worker_reads",
                 health_status: :dead
               )
    end

    test "paginates deterministically by heartbeat and UUID without aggregating unpaged workers" do
      create_flow("worker_cursor")
      timestamp = ~U[2026-08-28 12:00:00.000000Z]

      first = "ffffffff-ffff-ffff-ffff-fffffffffff3"
      second = "ffffffff-ffff-ffff-ffff-fffffffffff2"
      third = "ffffffff-ffff-ffff-ffff-fffffffffff1"

      Enum.each(
        [first, second, third],
        &insert_worker(&1, "worker_cursor", heartbeat_at: timestamp)
      )

      assert {:ok, [%WorkerSummary{worker_id: ^first}, %WorkerSummary{worker_id: ^second}]} =
               Workers.list(TestRepo, flow_slug: "worker_cursor", limit: 2)

      assert {:ok, [%WorkerSummary{worker_id: ^third}]} =
               Workers.list(TestRepo, flow_slug: "worker_cursor", cursor: second, limit: 2)

      assert {:error, :invalid_id} = Workers.list(TestRepo, cursor: "bad-cursor")
    end

    test "rejects invalid identifiers and reports missing workers" do
      assert {:error, :invalid_id} = Workers.get(TestRepo, "not-a-uuid")
      assert {:error, :not_found} = Workers.get(TestRepo, Ecto.UUID.generate())
    end
  end

  describe "list_tasks/3" do
    test "returns complete typed tasks actually owned by the worker in deterministic order" do
      create_flow("worker_tasks")
      add_step("worker_tasks", "work")

      worker_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee1"
      other_worker_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee2"
      insert_worker(worker_id, "worker_tasks")
      insert_worker(other_worker_id, "worker_tasks")

      older_run_id = start_flow_run("worker_tasks", %{"order" => "older"})
      newer_run_id = start_flow_run("worker_tasks", %{"order" => "newer"})
      other_run_id = start_flow_run("worker_tasks", %{"order" => "other"})

      update_task(older_run_id, "completed", worker_id, hours_ago: 2)
      update_task(newer_run_id, "completed", worker_id, hours_ago: 1)
      update_task(other_run_id, "completed", other_worker_id, hours_ago: 0)

      assert {:ok, tasks} = Workers.list_tasks(TestRepo, worker_id, limit: 10)
      assert Enum.map(tasks, & &1.run_id) == [newer_run_id, older_run_id]
      assert Enum.all?(tasks, &match?(%StepTask{last_worker_id: ^worker_id}, &1))

      assert {:ok, [%StepTask{run_id: ^newer_run_id}]} =
               Workers.list_tasks(TestRepo, worker_id, limit: 1)

      assert {:error, :invalid_id} = Workers.list_tasks(TestRepo, "not-a-uuid", [])
    end

    test "breaks equal lifecycle timestamps by run UUID, step slug, and task index" do
      create_flow("worker_task_ties")
      add_step("worker_task_ties", "alpha")
      add_step("worker_task_ties", "fanout", type: "map")

      worker_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee3"
      insert_worker(worker_id, "worker_task_ties")

      run_ids = [
        start_flow_run("worker_task_ties", ["a", "b"]),
        start_flow_run("worker_task_ties", ["c", "d"])
      ]

      event_at = ~U[2026-08-28 12:00:00.000000Z]
      Enum.each(run_ids, &update_task(&1, "completed", worker_id, event_at: event_at))

      [first_run_id, second_run_id] = Enum.sort(run_ids, :desc)

      assert {:ok, tasks} = Workers.list_tasks(TestRepo, worker_id, limit: 10)

      assert Enum.map(tasks, &{&1.run_id, &1.step_slug, &1.task_index}) == [
               {first_run_id, "alpha", 0},
               {first_run_id, "fanout", 0},
               {first_run_id, "fanout", 1},
               {second_run_id, "alpha", 0},
               {second_run_id, "fanout", 0},
               {second_run_id, "fanout", 1}
             ]
    end
  end

  describe "adjacent/3" do
    test "navigates newest-first with UUID tie-breaks" do
      create_flow("worker_adjacent")
      timestamp = ~U[2026-08-28 12:00:00.000000Z]

      first = "dddddddd-dddd-dddd-dddd-ddddddddddd3"
      second = "dddddddd-dddd-dddd-dddd-ddddddddddd2"
      third = "dddddddd-dddd-dddd-dddd-ddddddddddd1"

      Enum.each(
        [first, second, third],
        &insert_worker(&1, "worker_adjacent", heartbeat_at: timestamp)
      )

      assert {:ok, ^second} = Workers.adjacent(TestRepo, first, :next)
      assert {:ok, ^first} = Workers.adjacent(TestRepo, second, :prev)
      assert {:ok, ^third} = Workers.adjacent(TestRepo, second, :next)
      assert {:error, :not_found} = Workers.adjacent(TestRepo, third, :next)
      assert {:error, :not_found} = Workers.adjacent(TestRepo, Ecto.UUID.generate(), :next)
      assert {:error, :invalid_id} = Workers.adjacent(TestRepo, "not-a-uuid", :next)
      assert {:error, :invalid_direction} = Workers.adjacent(TestRepo, first, :sideways)
    end
  end

  describe "delete/2" do
    test "is idempotent and does not delete tasks or runs" do
      create_flow("worker_delete")
      add_step("worker_delete", "work")
      worker_id = "cccccccc-cccc-cccc-cccc-ccccccccccc1"
      insert_worker(worker_id, "worker_delete")
      run_id = start_flow_run("worker_delete", %{})
      update_task(run_id, "started", worker_id)

      assert :ok = Workers.delete(TestRepo, worker_id)
      assert :ok = Workers.delete(TestRepo, worker_id)
      assert {:error, :not_found} = Workers.get(TestRepo, worker_id)

      assert {:ok, [%StepTask{last_worker_id: nil}]} =
               PgFlow.Runs.list_step_tasks(TestRepo, run_id, "work")

      assert {:ok, %{run_id: ^run_id}} = PgFlow.Runs.get(TestRepo, run_id)
      assert {:error, :invalid_id} = Workers.delete(TestRepo, "not-a-uuid")
    end
  end

  describe "test database reset" do
    test "removes persisted workers" do
      worker_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1"
      insert_worker(worker_id, "worker_reset")

      TestRepo.query!("SELECT pgflow_tests.reset_db()")

      assert {:error, :not_found} = Workers.get(TestRepo, worker_id)
    end
  end

  defp insert_worker(worker_id, flow_slug, opts \\ []) do
    heartbeat_at =
      Keyword.get_lazy(opts, :heartbeat_at, fn ->
        DateTime.add(DateTime.utc_now(), -Keyword.get(opts, :seconds_ago, 0), :second)
      end)

    stopped_at = if Keyword.get(opts, :stopped, false), do: heartbeat_at
    deprecated_at = if Keyword.get(opts, :deprecated, false), do: heartbeat_at

    TestRepo.query!(
      """
      INSERT INTO pgflow.workers
        (worker_id, queue_name, function_name, started_at, last_heartbeat_at, stopped_at, deprecated_at)
      VALUES ($1, $2, 'Elixir.Worker.perform/2', $3, $3, $4, $5)
      """,
      [Ecto.UUID.dump!(worker_id), flow_slug, heartbeat_at, stopped_at, deprecated_at]
    )

    worker_id
  end

  defp update_task(run_id, status, worker_id, opts \\ []) do
    event_at =
      Keyword.get_lazy(opts, :event_at, fn ->
        DateTime.add(DateTime.utc_now(), -Keyword.get(opts, :hours_ago, 0), :hour)
      end)

    queued_at = DateTime.add(event_at, -1, :second)

    completed_at = if status == "completed", do: event_at
    started_at = if status in ["started", "completed"], do: event_at

    TestRepo.query!(
      """
      UPDATE pgflow.step_tasks
      SET status = $2, last_worker_id = $3, queued_at = $4, started_at = $5, completed_at = $6
      WHERE run_id = $1
      """,
      [
        Ecto.UUID.dump!(run_id),
        status,
        Ecto.UUID.dump!(worker_id),
        queued_at,
        started_at,
        completed_at
      ]
    )
  end
end
