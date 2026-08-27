defmodule PgFlow.Worker.StalledTaskRecoveryTest do
  @moduledoc """
  Tests for PgFlow.Worker.StalledTaskRecovery and the step-aware
  `pgflow.recover_stalled_tasks/1` helper it drives.

  The helper deadlines on each task's effective timeout
  (`coalesce(step.opt_timeout, flow.opt_timeout)`) plus a buffer, caps requeues
  at 3 (then archives + marks `permanently_stalled_at`), takes
  `FOR UPDATE SKIP LOCKED`, and skips tasks whose run has failed.
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Queries.Flows
  alias PgFlow.Queries.Workers, as: WorkerQueries
  alias PgFlow.TestRepo
  alias PgFlow.Worker.StalledTaskRecovery

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    on_exit(fn ->
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  # Single-step flow, flow timeout defaults to 30s; the step inherits it.
  defmodule StalledFlow do
    use PgFlow.Flow
    @flow slug: :stalled_flow, max_attempts: 3

    step :process do
      fn input, _ctx -> %{result: input["value"]} end
    end
  end

  # A second flow whose step is ALSO named `process` — guards the recovery
  # join from matching on step_slug without flow_slug.
  defmodule SecondFlow do
    use PgFlow.Flow
    @flow slug: :second_flow, max_attempts: 3

    step :process do
      fn input, _ctx -> %{result: input["value"]} end
    end
  end

  # --- helpers ---

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    TestRepo.query!("SELECT pgflow.create_flow($1, $2, $3, $4)", [
      flow_slug,
      definition.opts[:max_attempts] || 3,
      definition.opts[:base_delay] || 1,
      definition.opts[:timeout] || 30
    ])

    for step <- definition.steps do
      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          Atom.to_string(step.slug),
          Enum.map(step.depends_on, &Atom.to_string/1),
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          Atom.to_string(step.step_type)
        ]
      )
    end

    flow_slug
  end

  defp start_flow_run(flow_slug, input) do
    %{rows: [[result]]} =
      TestRepo.query!("SELECT pgflow.start_flow($1, cast($2 as text)::jsonb)", [
        flow_slug,
        Jason.encode!(input)
      ])

    case result do
      {run_id, _, _, _, _, _, _, _, _} -> Ecto.UUID.load!(run_id)
      _ -> raise "Unexpected result: #{inspect(result)}"
    end
  end

  # Compile a flow, start a run, and move its first task to 'started'.
  defp setup_started(flow_module \\ StalledFlow, input \\ %{"value" => 42}) do
    flow_slug = compile_flow(flow_module)
    run_id = start_flow_run(flow_slug, input)
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 10)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    worker_id = Ecto.UUID.generate()
    {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")
    {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)
    {:ok, run_id_bin} = Ecto.UUID.dump(run_id)
    %{flow_slug: flow_slug, msg_ids: msg_ids, run_id_bin: run_id_bin, run_id: run_id}
  end

  defp set_flow_timeout(slug, secs),
    do:
      TestRepo.query!("UPDATE pgflow.flows SET opt_timeout = $2 WHERE flow_slug = $1", [
        slug,
        secs
      ])

  defp set_step_timeout(slug, step, secs),
    do:
      TestRepo.query!(
        "UPDATE pgflow.steps SET opt_timeout = $3 WHERE flow_slug = $1 AND step_slug = $2",
        [slug, step, secs]
      )

  # Backdate so the task looks started `secs` ago (queued slightly earlier to
  # satisfy started_at >= queued_at).
  defp backdate(run_id_bin, secs),
    do:
      TestRepo.query!(
        "UPDATE pgflow.step_tasks SET queued_at = NOW() - (($2 + 5) * interval '1 second'), " <>
          "started_at = NOW() - ($2 * interval '1 second') WHERE run_id = $1",
        [run_id_bin, secs]
      )

  defp task(run_id_bin, fields, step \\ "process", idx \\ 0) do
    %{rows: [row]} =
      TestRepo.query!(
        "SELECT #{fields} FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = $2 AND task_index = $3",
        [run_id_bin, step, idx]
      )

    row
  end

  defp message_in_queue?(flow_slug, msg_ids) do
    {:ok, rows} = Flows.read(TestRepo, flow_slug, 0, 10)
    Enum.any?(rows, fn [m | _] -> m in msg_ids end)
  end

  defp run_status(run_id_bin) do
    %{rows: [[status]]} =
      TestRepo.query!("SELECT status FROM pgflow.runs WHERE run_id = $1", [run_id_bin])

    status
  end

  defp step_status(run_id_bin, step) do
    %{rows: [[status]]} =
      TestRepo.query!(
        "SELECT status FROM pgflow.step_states WHERE run_id = $1 AND step_slug = $2",
        [run_id_bin, step]
      )

    status
  end

  defp task_statuses(run_id_bin, step) do
    %{rows: rows} =
      TestRepo.query!(
        "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = $2 ORDER BY task_index",
        [run_id_bin, step]
      )

    List.flatten(rows)
  end

  # Builds `fanout -> items` where `items` is a 3-task map step whose
  # `when_exhausted` mode skips the whole step. Drives it to the moment right
  # after task 0 exhausts its single attempt: `items` is `skipped`, the run is
  # terminal, the two sibling messages are archived — and the sibling TASK rows
  # are still sitting in `started`, which is what a stalled sweep would see.
  defp setup_skipped_map(when_exhausted) do
    flow_slug = "skip_map_#{System.unique_integer([:positive])}"

    TestRepo.query!("SELECT pgflow.create_flow($1, 1, 1, 30)", [flow_slug])
    TestRepo.query!("SELECT pgflow.add_step($1, 'fanout', ARRAY[]::text[])", [flow_slug])

    TestRepo.query!(
      """
      SELECT pgflow.add_step(
        $1, 'items', ARRAY['fanout']::text[], 1, null, null, null, 'map',
        null, null, 'skip', $2
      )
      """,
      [flow_slug, when_exhausted]
    )

    run_id = start_flow_run(flow_slug, %{})
    {:ok, run_id_bin} = Ecto.UUID.dump(run_id)

    worker_id = Ecto.UUID.generate()
    {:ok, _} = WorkerQueries.register_worker(TestRepo, worker_id, flow_slug, "elixir:test")

    # fanout emits three elements, which fans `items` out to three tasks.
    start_all(flow_slug, worker_id)
    {:ok, _} = Flows.complete_task(TestRepo, run_id, "fanout", 0, [1, 2, 3])

    item_msg_ids = start_all(flow_slug, worker_id)
    assert task_statuses(run_id_bin, "items") == ~w(started started started)

    # Task 0 burns its only attempt: the step skips and its siblings' messages
    # are archived in the same transaction.
    {:ok, _} = Flows.fail_task(TestRepo, run_id, "items", 0, "item 1 exploded")

    %{
      flow_slug: flow_slug,
      run_id: run_id,
      run_id_bin: run_id_bin,
      item_msg_ids: item_msg_ids
    }
  end

  defp start_all(flow_slug, worker_id) do
    {:ok, messages} = Flows.read(TestRepo, flow_slug, 30, 10)
    msg_ids = Enum.map(messages, fn [msg_id | _] -> msg_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, flow_slug, msg_ids, worker_id)
    msg_ids
  end

  # --- detection & basic requeue ---

  describe "recover_stalled_tasks/2 — detection & basic requeue" do
    test "does not recover a freshly started task" do
      %{run_id_bin: rid} = setup_started()
      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid, "status") == ["started"]
    end

    test "recovers a task past its effective timeout + buffer, preserving attempts_count" do
      %{flow_slug: slug, msg_ids: msg_ids, run_id_bin: rid} = setup_started()
      [attempts_before] = task(rid, "attempts_count")

      # StalledFlow effective timeout 30 + buffer 60 = 90s deadline.
      backdate(rid, 120)
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)

      [status, started_at, last_worker_id, requeued_count, last_requeued_at, attempts_count] =
        task(
          rid,
          "status, started_at, last_worker_id, requeued_count, last_requeued_at, attempts_count"
        )

      assert status == "queued"
      assert is_nil(started_at)
      assert is_nil(last_worker_id)
      assert requeued_count == 1
      refute is_nil(last_requeued_at)
      assert attempts_count == attempts_before
      assert message_in_queue?(slug, msg_ids)

      # idempotent — no longer stalled
      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)
    end

    test "respects the buffer boundary" do
      %{run_id_bin: rid} = setup_started()

      # deadline = 30 (timeout) + 10 (buffer) = 40s. 39s in → safe; 41s in → stalled.
      backdate(rid, 39)
      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 10)

      backdate(rid, 41)
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 10)
    end
  end

  # --- step-aware deadline (our divergence from upstream's flow-only) ---

  describe "recover_stalled_tasks/2 — step-aware deadline" do
    test "does not recover a long step still within its step timeout (step > flow)" do
      %{flow_slug: slug, run_id_bin: rid} = setup_started()
      set_flow_timeout(slug, 30)
      set_step_timeout(slug, "process", 300)

      # 70s in, deadline = 300 + 60 = 360s — healthy.
      backdate(rid, 70)
      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid, "status") == ["started"]
    end

    test "recovers a long step once past its step timeout + buffer" do
      %{flow_slug: slug, run_id_bin: rid} = setup_started()
      set_flow_timeout(slug, 30)
      set_step_timeout(slug, "process", 300)

      backdate(rid, 370)
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
    end

    test "recovers a short step sooner than the flow default (step < flow)" do
      %{flow_slug: slug, run_id_bin: rid} = setup_started()
      set_flow_timeout(slug, 300)
      set_step_timeout(slug, "process", 30)

      # deadline = 30 + 60 = 90s. 100s in → stalled even though the flow default is 300.
      backdate(rid, 100)
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
    end

    test "falls back to the flow timeout when the step timeout is null" do
      %{flow_slug: slug, run_id_bin: rid} = setup_started()
      set_flow_timeout(slug, 30)
      set_step_timeout(slug, "process", nil)

      backdate(rid, 120)
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
    end
  end

  # --- requeue cap ---

  describe "recover_stalled_tasks/2 — requeue cap" do
    test "requeues below the cap and increments requeued_count" do
      %{run_id_bin: rid} = setup_started()
      TestRepo.query!("UPDATE pgflow.step_tasks SET requeued_count = 2 WHERE run_id = $1", [rid])
      backdate(rid, 120)

      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid, "status, requeued_count") == ["queued", 3]
    end

    test "archives and permanently marks once the cap is reached, then ignores it" do
      %{flow_slug: slug, msg_ids: msg_ids, run_id_bin: rid} = setup_started()
      TestRepo.query!("UPDATE pgflow.step_tasks SET requeued_count = 3 WHERE run_id = $1", [rid])
      backdate(rid, 120)

      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)

      [status, requeued_count, permanently_stalled_at] =
        task(rid, "status, requeued_count, permanently_stalled_at")

      assert status == "started"
      assert requeued_count == 3
      refute is_nil(permanently_stalled_at)
      refute message_in_queue?(slug, msg_ids)

      # excluded from future sweeps
      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)
    end
  end

  # --- forcing & mixed sweeps ---

  describe "recover_stalled_tasks/2 — forcing & mixed sweeps" do
    test "forces the archive even when nothing is requeued (only-archive sweep)" do
      %{flow_slug: slug, msg_ids: msg_ids, run_id_bin: rid} = setup_started()
      TestRepo.query!("UPDATE pgflow.step_tasks SET requeued_count = 3 WHERE run_id = $1", [rid])
      backdate(rid, 120)

      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)
      refute message_in_queue?(slug, msg_ids)
      refute is_nil(hd(task(rid, "permanently_stalled_at")))
    end

    test "requeues and archives in the same sweep" do
      %{run_id_bin: rid_a} = setup_started(StalledFlow)
      %{run_id_bin: rid_b} = setup_started(SecondFlow)

      # A is requeuable, B is at the cap.
      TestRepo.query!("UPDATE pgflow.step_tasks SET requeued_count = 3 WHERE run_id = $1", [rid_b])

      backdate(rid_a, 120)
      backdate(rid_b, 120)

      # Count reflects only the requeued task.
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid_a, "status") == ["queued"]
      assert task(rid_b, "status") == ["started"]
      refute is_nil(hd(task(rid_b, "permanently_stalled_at")))
    end
  end

  # --- multiple flows ---

  describe "recover_stalled_tasks/2 — multiple flows" do
    test "recovers only flows past their own timeout" do
      %{run_id_bin: fast} = setup_started(StalledFlow)
      %{flow_slug: slow_slug, run_id_bin: slow} = setup_started(SecondFlow)
      set_flow_timeout(slow_slug, 600)
      set_step_timeout(slow_slug, "process", 600)

      backdate(fast, 120)
      backdate(slow, 120)

      # fast (30+60=90) stalled; slow (600+60=660) healthy.
      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(fast, "status") == ["queued"]
      assert task(slow, "status") == ["started"]
    end

    test "distinguishes the same step_slug across different flows" do
      %{run_id_bin: a} = setup_started(StalledFlow)
      %{flow_slug: b_slug, run_id_bin: b} = setup_started(SecondFlow)
      set_step_timeout(b_slug, "process", 600)
      set_flow_timeout(b_slug, 600)

      backdate(a, 120)
      backdate(b, 120)

      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(a, "status") == ["queued"]
      assert task(b, "status") == ["started"]
    end
  end

  # --- edge cases ---

  describe "recover_stalled_tasks/2 — edge cases" do
    test "does not requeue tasks whose run has failed" do
      %{run_id_bin: rid} = setup_started()
      backdate(rid, 120)

      TestRepo.query!(
        "UPDATE pgflow.runs SET status = 'failed', failed_at = NOW() WHERE run_id = $1",
        [rid]
      )

      assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid, "status") == ["failed"]
    end

    test "requeues a task with a null message_id without error" do
      %{run_id_bin: rid} = setup_started()
      TestRepo.query!("UPDATE pgflow.step_tasks SET message_id = NULL WHERE run_id = $1", [rid])
      backdate(rid, 120)

      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid, "status") == ["queued"]
    end

    test "recovers every task_index of a fanned-out step" do
      %{msg_ids: msg_ids, run_id_bin: rid} = setup_started()

      # Simulate a second map task (task_index 1) sharing the step.
      TestRepo.query!(
        """
        INSERT INTO pgflow.step_tasks
          (flow_slug, run_id, step_slug, message_id, task_index, status, attempts_count, queued_at, started_at, requeued_count)
        SELECT flow_slug, run_id, step_slug, $2::bigint, 1, 'started', attempts_count, queued_at, started_at, requeued_count
        FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'process' AND task_index = 0
        """,
        [rid, hd(msg_ids) + 100_000]
      )

      backdate(rid, 120)

      assert {:ok, 2} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(rid, "status", "process", 0) == ["queued"]
      assert task(rid, "status", "process", 1) == ["queued"]
    end
  end

  # --- skipped steps ---
  #
  # `fail_task`'s when_exhausted skip archives the sibling MESSAGES but leaves
  # the sibling `step_tasks` rows in 'started' forever — nothing in the vendored
  # core ever terminalizes them. A stalled sweep must not mistake those orphans
  # for a crashed worker's work and hand them back to the queue.

  describe "recover_stalled_tasks/2 — skipped steps" do
    for mode <- ["skip", "skip-cascade"] do
      test "leaves the stranded siblings of a #{mode}ped map step alone" do
        %{flow_slug: slug, run_id_bin: rid, item_msg_ids: msg_ids} =
          setup_skipped_map(unquote(mode))

        assert step_status(rid, "items") == "skipped"
        assert run_status(rid) == "completed"
        # The premise: siblings are left non-terminal by the skip.
        assert task_statuses(rid, "items") == ~w(failed started started)

        backdate(rid, 120)
        assert {:ok, 0} = Flows.recover_stalled_tasks(TestRepo, 60)

        refute "queued" in task_statuses(rid, "items")
        assert run_status(rid) == "completed"
        assert step_status(rid, "items") == "skipped"
        refute message_in_queue?(slug, msg_ids)
      end
    end

    test "still recovers a genuinely stalled task on a live step in the same sweep" do
      %{run_id_bin: skipped_rid} = setup_skipped_map("skip")
      %{run_id_bin: live_rid} = setup_started()

      backdate(skipped_rid, 120)
      backdate(live_rid, 120)

      assert {:ok, 1} = Flows.recover_stalled_tasks(TestRepo, 60)
      assert task(live_rid, "status") == ["queued"]
      refute "queued" in task_statuses(skipped_rid, "items")
    end
  end

  # --- GenServer integration ---

  describe "GenServer periodic recovery" do
    test "periodic timer fires and recovers stalled tasks" do
      %{run_id_bin: rid} = setup_started()
      backdate(rid, 120)

      {:ok, pid} =
        StalledTaskRecovery.start_link(
          repo: TestRepo,
          recovery_interval: 100,
          stale_threshold: 60
        )

      Process.sleep(500)
      assert task(rid, "status") == ["queued"]

      GenServer.stop(pid)
    end

    test "survives stray messages and still handles recovery" do
      # Same defect class as PgFlow.Worker.Server: this GenServer defines
      # handle_info/2 clauses without a catch-all, so any message it wasn't
      # expecting (e.g. Swoosh Test adapter's $callers broadcast) crashes it.
      %{run_id_bin: rid} = setup_started()
      backdate(rid, 120)

      {:ok, pid} =
        StalledTaskRecovery.start_link(
          repo: TestRepo,
          recovery_interval: 60_000,
          stale_threshold: 60
        )

      send(pid, {:email, :garbage})
      send(pid, :unexpected)
      _ = :sys.get_state(pid)

      send(pid, :recover)
      _ = :sys.get_state(pid)

      assert task(rid, "status") == ["queued"]

      GenServer.stop(pid)
    end
  end
end
