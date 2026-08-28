defmodule PgFlow.RunsTest do
  use PgFlow.IntegrationCase

  alias PgFlow.{RunHistoryCell, Runs, RunSummary}
  alias PgFlow.Schema.{Run, StepState, StepTask}

  describe "get/2 and get_with_states/2" do
    test "return typed runs and reject invalid or missing identifiers" do
      create_flow("run_reads")
      add_step("run_reads", "first")
      run_id = start_flow_run("run_reads", %{"items" => [1, 2]})

      assert {:ok, %Run{run_id: ^run_id, input: %{"items" => [1, 2]}}} =
               Runs.get(TestRepo, run_id)

      assert {:error, :invalid_id} = Runs.get(TestRepo, "not-a-uuid")
      assert {:error, :not_found} = Runs.get(TestRepo, Ecto.UUID.generate())
    end

    test "preloads ordered states with list-valued JSON output" do
      create_flow("run_states")
      add_step("run_states", "zeta")
      add_step("run_states", "alpha")
      run_id = start_flow_run("run_states", %{})

      TestRepo.query!(
        """
        UPDATE pgflow.step_states
        SET status = 'completed', remaining_tasks = 0, started_at = created_at,
            completed_at = created_at, output = '[1, 2, 3]'::jsonb
        WHERE run_id = $1 AND step_slug = 'alpha'
        """,
        [Ecto.UUID.dump!(run_id)]
      )

      assert {:ok, %Run{step_states: states}} = Runs.get_with_states(TestRepo, run_id)
      assert Enum.map(states, & &1.step_slug) == ["alpha", "zeta"]
      assert %StepState{output: [1, 2, 3]} = hd(states)

      assert {:error, :invalid_id} = Runs.get_with_states(TestRepo, "not-a-uuid")
      assert {:error, :not_found} = Runs.get_with_states(TestRepo, Ecto.UUID.generate())
    end
  end

  describe "list/2 and count/2" do
    test "return typed summaries and apply flow, status, type, time, and JSON containment filters" do
      create_flow("matching_flow")
      add_step("matching_flow", "work")
      create_flow("other_flow")
      add_step("other_flow", "work")
      TestRepo.query!("UPDATE pgflow.flows SET flow_type = 'job' WHERE flow_slug = 'other_flow'")

      matching_id =
        start_flow_run("matching_flow", %{"tenant" => "acme", "nested" => %{"tier" => 2}})

      completed_id = start_flow_run("matching_flow", %{"tenant" => "acme"})
      job_id = start_flow_run("other_flow", %{"tenant" => "acme"})
      old_id = start_flow_run("matching_flow", %{"tenant" => "old"})

      TestRepo.query!(
        "UPDATE pgflow.runs SET status = 'completed', remaining_steps = 0, completed_at = started_at WHERE run_id = $1",
        [Ecto.UUID.dump!(completed_id)]
      )

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = now() - interval '2 hours' WHERE run_id = $1",
        [
          Ecto.UUID.dump!(old_id)
        ]
      )

      assert {:ok, [%RunSummary{run_id: ^matching_id, flow_type: "flow"}]} =
               Runs.list(TestRepo,
                 flow_slug: "matching_flow",
                 status: :started,
                 flow_type: "flow",
                 time_range: :last_hour,
                 input_contains: %{"nested" => %{"tier" => 2}}
               )

      assert {:ok, 1} =
               Runs.count(TestRepo,
                 flow_slug: "matching_flow",
                 status: "completed",
                 input_contains: %{"tenant" => "acme"}
               )

      assert {:ok, [%RunSummary{run_id: ^job_id}]} = Runs.list(TestRepo, flow_type: :job)
    end

    test "uses a deterministic composite cursor when timestamps match" do
      create_flow("cursor_runs")
      add_step("cursor_runs", "work")
      run_ids = Enum.map(1..3, fn index -> start_flow_run("cursor_runs", %{"index" => index}) end)
      timestamp = DateTime.utc_now()

      TestRepo.query!("UPDATE pgflow.runs SET started_at = $1 WHERE flow_slug = 'cursor_runs'", [
        timestamp
      ])

      [first, second, third] = Enum.sort(run_ids, :desc)

      assert {:ok, [%RunSummary{run_id: ^first}, %RunSummary{run_id: ^second}]} =
               Runs.list(TestRepo, flow_slug: "cursor_runs", limit: 2)

      assert {:ok, [%RunSummary{run_id: ^third}]} =
               Runs.list(TestRepo, flow_slug: "cursor_runs", cursor: second, limit: 2)

      assert {:error, :invalid_id} = Runs.list(TestRepo, cursor: "bad-cursor")
    end

    test "does not impose a time window unless one is requested" do
      create_flow("all_time_runs")
      add_step("all_time_runs", "work")
      run_id = start_flow_run("all_time_runs", %{"scope" => "all-time"})

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = now() - interval '2 days' WHERE run_id = $1",
        [Ecto.UUID.dump!(run_id)]
      )

      assert {:ok, [%RunSummary{run_id: ^run_id}]} =
               Runs.list(TestRepo,
                 flow_slug: "all_time_runs",
                 input_contains: %{"scope" => "all-time"}
               )

      assert {:ok, 1} = Runs.count(TestRepo, flow_slug: "all_time_runs")
      assert {:ok, []} = Runs.list(TestRepo, flow_slug: "all_time_runs", time_range: :last_24h)
    end

    test "uses failed_at to bound failed-run duration" do
      create_flow("failed_duration")
      add_step("failed_duration", "work")
      run_id = start_flow_run("failed_duration", %{})

      TestRepo.query!(
        """
        UPDATE pgflow.runs
        SET status = 'failed', remaining_steps = 0,
            started_at = now() - interval '10 minutes',
            failed_at = now() - interval '5 minutes'
        WHERE run_id = $1
        """,
        [Ecto.UUID.dump!(run_id)]
      )

      assert {:ok, [%RunSummary{duration_ms: duration_ms}]} =
               Runs.list(TestRepo, flow_slug: "failed_duration")

      assert Decimal.compare(duration_ms, Decimal.new(299_000)) in [:eq, :gt]
      assert Decimal.compare(duration_ms, Decimal.new(301_000)) in [:eq, :lt]
    end

    test "started_after takes precedence over the named time range" do
      create_flow("explicit_time")
      add_step("explicit_time", "work")
      run_id = start_flow_run("explicit_time", %{})

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = now() - interval '2 hours' WHERE run_id = $1",
        [Ecto.UUID.dump!(run_id)]
      )

      assert {:ok, []} = Runs.list(TestRepo, flow_slug: "explicit_time", time_range: :last_hour)

      assert {:ok, [%RunSummary{run_id: ^run_id}]} =
               Runs.list(TestRepo,
                 flow_slug: "explicit_time",
                 time_range: :last_hour,
                 started_after: DateTime.add(DateTime.utc_now(), -3, :hour)
               )
    end

    test "started_after applies an inclusive lower bound to list and count" do
      create_flow("inclusive_lower_bound")
      add_step("inclusive_lower_bound", "work")
      run_id = start_flow_run("inclusive_lower_bound", %{})
      boundary = ~U[2026-08-28 12:00:00.000000Z]

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = $2 WHERE run_id = $1",
        [Ecto.UUID.dump!(run_id), boundary]
      )

      assert {:ok, [%RunSummary{run_id: ^run_id}]} =
               Runs.list(TestRepo,
                 flow_slug: "inclusive_lower_bound",
                 started_after: boundary
               )

      assert {:ok, 1} =
               Runs.count(TestRepo,
                 flow_slug: "inclusive_lower_bound",
                 started_after: boundary
               )

      assert {:ok, []} =
               Runs.list(TestRepo,
                 flow_slug: "inclusive_lower_bound",
                 started_after: DateTime.add(boundary, 1, :second)
               )
    end

    test "started_before applies an inclusive upper bound to list and count" do
      create_flow("bounded_time")
      add_step("bounded_time", "work")
      run_id = start_flow_run("bounded_time", %{})
      boundary = DateTime.add(DateTime.utc_now(), -10, :minute)

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = $2 WHERE run_id = $1",
        [Ecto.UUID.dump!(run_id), boundary]
      )

      assert {:ok, [%RunSummary{run_id: ^run_id}]} =
               Runs.list(TestRepo, flow_slug: "bounded_time", started_before: boundary)

      assert {:ok, 1} =
               Runs.count(TestRepo, flow_slug: "bounded_time", started_before: boundary)

      assert {:ok, []} =
               Runs.list(TestRepo,
                 flow_slug: "bounded_time",
                 started_before: DateTime.add(boundary, -1, :second)
               )
    end
  end

  describe "step state and task reads" do
    test "return ordered schema structs with every persisted task field" do
      create_flow("task_reads")
      add_step("task_reads", "fanout", type: "map")
      run_id = start_flow_run("task_reads", ["a", "b"])

      worker_id = Ecto.UUID.generate()
      queued_at = ~U[2026-08-28 12:00:00.000000Z]
      started_at = ~U[2026-08-28 12:01:00.000000Z]
      failed_at = ~U[2026-08-28 12:02:00.000000Z]
      last_requeued_at = ~U[2026-08-28 12:00:30.000000Z]
      permanently_stalled_at = ~U[2026-08-28 12:03:00.000000Z]

      TestRepo.query!(
        """
        INSERT INTO pgflow.workers (worker_id, queue_name, function_name)
        VALUES ($1, 'task_reads', 'Elixir.TaskReads.perform/2')
        """,
        [Ecto.UUID.dump!(worker_id)]
      )

      TestRepo.query!(
        """
        UPDATE pgflow.step_tasks
        SET message_id = 999, status = 'failed', attempts_count = 3,
            error_message = 'provider unavailable', output = '{"partial":[1,2]}'::jsonb,
            queued_at = $2, started_at = $3, completed_at = NULL, failed_at = $4,
            last_worker_id = $5, requeued_count = 2, last_requeued_at = $6,
            permanently_stalled_at = $7
        WHERE run_id = $1 AND step_slug = 'fanout' AND task_index = 1
        """,
        [
          Ecto.UUID.dump!(run_id),
          queued_at,
          started_at,
          failed_at,
          Ecto.UUID.dump!(worker_id),
          last_requeued_at,
          permanently_stalled_at
        ]
      )

      assert {:ok, [%StepState{step_slug: "fanout"}]} = Runs.list_step_states(TestRepo, run_id)
      assert {:ok, tasks} = Runs.list_step_tasks(TestRepo, run_id, "fanout")
      assert Enum.map(tasks, & &1.task_index) == [0, 1]

      assert {:ok, run_tasks} = Runs.list_run_tasks(TestRepo, run_id)
      assert Enum.map(run_tasks, &{&1.step_slug, &1.task_index}) == [{"fanout", 0}, {"fanout", 1}]
      assert Enum.all?(run_tasks, &match?(%StepTask{}, &1))

      assert {:ok, task} = Runs.get_step_task(TestRepo, run_id, "fanout", 1)

      assert %StepTask{
               flow_slug: "task_reads",
               run_id: ^run_id,
               step_slug: "fanout",
               message_id: 999,
               task_index: 1,
               status: "failed",
               attempts_count: 3,
               error_message: "provider unavailable",
               output: %{"partial" => [1, 2]},
               queued_at: ^queued_at,
               completed_at: nil,
               failed_at: ^failed_at,
               started_at: ^started_at,
               last_worker_id: ^worker_id,
               requeued_count: 2,
               last_requeued_at: ^last_requeued_at,
               permanently_stalled_at: ^permanently_stalled_at
             } = task

      assert {:error, :not_found} = Runs.get_step_task(TestRepo, run_id, "fanout", 8)
      assert {:error, :invalid_id} = Runs.list_step_states(TestRepo, "bad-id")
      assert {:error, :invalid_id} = Runs.list_run_tasks(TestRepo, "bad-id")
      assert {:error, :invalid_id} = Runs.list_step_tasks(TestRepo, "bad-id", "fanout")
      assert {:error, :invalid_id} = Runs.get_step_task(TestRepo, "bad-id", "fanout", 0)
    end
  end

  describe "adjacent/3" do
    test "navigates deterministically across runs with identical timestamps" do
      create_flow("adjacent_runs")
      add_step("adjacent_runs", "work")

      run_ids =
        Enum.map(1..3, fn index -> start_flow_run("adjacent_runs", %{"index" => index}) end)

      timestamp = ~U[2026-08-28 12:00:00.000000Z]

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = $1 WHERE flow_slug = 'adjacent_runs'",
        [timestamp]
      )

      [newest, middle, oldest] = Enum.sort(run_ids, :desc)
      assert {:ok, ^oldest} = Runs.adjacent(TestRepo, middle, :next)
      assert {:ok, ^newest} = Runs.adjacent(TestRepo, middle, :prev)
      assert {:error, :not_found} = Runs.adjacent(TestRepo, oldest, :next)
      assert {:error, :not_found} = Runs.adjacent(TestRepo, newest, :prev)
      assert {:error, :invalid_id} = Runs.adjacent(TestRepo, "bad-id", :next)
      assert {:error, :invalid_direction} = Runs.adjacent(TestRepo, middle, :sideways)
    end
  end

  describe "history/3" do
    test "returns typed history cells in deterministic run and step order" do
      create_flow("history_runs")
      add_step("history_runs", "zeta")
      add_step("history_runs", "alpha")
      older_id = start_flow_run("history_runs", %{"position" => "older"})
      newer_id = start_flow_run("history_runs", %{"position" => "newer"})

      TestRepo.query!(
        "UPDATE pgflow.runs SET started_at = now() - interval '1 minute' WHERE run_id = $1",
        [
          Ecto.UUID.dump!(older_id)
        ]
      )

      assert {:ok, cells} = Runs.history(TestRepo, "history_runs", limit: 2)
      assert Enum.all?(cells, &match?(%RunHistoryCell{}, &1))

      assert Enum.map(cells, &{&1.run_id, &1.step_slug}) == [
               {newer_id, "alpha"},
               {newer_id, "zeta"},
               {older_id, "alpha"},
               {older_id, "zeta"}
             ]
    end
  end
end
