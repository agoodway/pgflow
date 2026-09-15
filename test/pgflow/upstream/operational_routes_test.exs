defmodule PgFlow.Upstream.OperationalRoutesTest do
  @moduledoc """
  Operational queue routes derive from persisted `step_tasks.queue_name`, not flow slug.
  """
  use PgFlow.IntegrationCase, async: false

  alias PgFlow.Queries.{Flows, Pgmq}
  alias PgFlow.{Runs, Workers}
  alias PgFlow.Signal.Notify

  @moduletag :integration
  @shared_msg_id 9_007_199_254_740_993

  describe "multi-route run operations" do
    setup do
      flow_slug = "multi_route_flow"
      create_flow(flow_slug)
      ensure_queue!("route_a")
      ensure_queue!("route_b")
      add_step(flow_slug, "alpha")
      add_step(flow_slug, "beta")

      TestRepo.query!(
        "UPDATE pgflow.steps SET queue_name = $2 WHERE flow_slug = $1 AND step_slug = $3",
        [flow_slug, "route_a", "alpha"]
      )

      TestRepo.query!(
        "UPDATE pgflow.steps SET queue_name = $2 WHERE flow_slug = $1 AND step_slug = $3",
        [flow_slug, "route_b", "beta"]
      )

      run_id = start_flow_run(flow_slug, %{})

      alpha_msg = task_message_id(run_id, "alpha")
      beta_msg = task_message_id(run_id, "beta")

      align_message_id!("route_a", run_id, "alpha", alpha_msg, @shared_msg_id)
      align_message_id!("route_b", run_id, "beta", beta_msg, @shared_msg_id)

      unrelated_run = start_flow_run(flow_slug, %{"other" => true})
      unrelated_msg = task_message_id(unrelated_run, "alpha")

      {:ok,
       flow_slug: flow_slug,
       run_id: run_id,
       unrelated_run: unrelated_run,
       unrelated_msg: unrelated_msg}
    end

    test "recovery resets only the stalled route when message ids collide", %{
      flow_slug: slug,
      run_id: run_id
    } do
      worker_id = Ecto.UUID.generate()

      {:ok, _} =
        PgFlow.Queries.Workers.register_worker(TestRepo, worker_id, "route_a", "elixir:test")

      assert {:ok, [_]} =
               Flows.start_tasks(TestRepo, slug, [@shared_msg_id], worker_id, "route_a")

      assert {:ok, [_]} =
               Flows.start_tasks(TestRepo, slug, [@shared_msg_id], worker_id, "route_b")

      TestRepo.query!(
        "UPDATE pgmq.q_route_a SET vt = now() + interval '1 hour' WHERE msg_id = $1",
        [@shared_msg_id]
      )

      TestRepo.query!(
        "UPDATE pgmq.q_route_b SET vt = now() + interval '1 hour' WHERE msg_id = $1",
        [@shared_msg_id]
      )

      TestRepo.query!(
        """
        UPDATE pgflow.step_tasks SET queued_at = now() - interval '2 hours',
          started_at = now() - interval '1 hour'
        WHERE run_id = $1 AND queue_name = 'route_a'
        """,
        [Ecto.UUID.dump!(run_id)]
      )

      assert %{rows: [[1, 1]]} = TestRepo.query!("SELECT * FROM pgflow.recover_stalled_tasks(0)")

      assert %{rows: [["queued"], ["started"]]} =
               TestRepo.query!(
                 "SELECT status FROM pgflow.step_tasks WHERE run_id = $1 ORDER BY queue_name",
                 [Ecto.UUID.dump!(run_id)]
               )

      assert %{rows: [[true]]} =
               TestRepo.query!(
                 "SELECT vt <= clock_timestamp() FROM pgmq.q_route_a WHERE msg_id = $1",
                 [
                   @shared_msg_id
                 ]
               )

      assert %{rows: [[true]]} =
               TestRepo.query!("SELECT vt > now() FROM pgmq.q_route_b WHERE msg_id = $1", [
                 @shared_msg_id
               ])
    end

    test "delay_run groups set_vt_batch by persisted queue", %{
      flow_slug: flow_slug,
      run_id: run_id
    } do
      {queue_name, message_id} = first_queued_task(run_id)

      assert :ok = Flows.delay_run(TestRepo, flow_slug, run_id, 30)

      assert delayed_in_queue?(queue_name, message_id)
      refute message_exists?("multi_route_flow", message_id)
    end

    test "make_available updates visibility per queue and message id", %{
      run_id: run_id,
      unrelated_run: unrelated_run
    } do
      other_alpha = task_message_id(unrelated_run, "alpha")

      hide_messages!("route_a", [@shared_msg_id, other_alpha])
      hide_messages!("route_b", [@shared_msg_id])

      assert :ok = Runs.make_available(TestRepo, run_id)

      assert visible_in_queue?("route_a", @shared_msg_id)
      assert visible_in_queue?("route_b", @shared_msg_id)
      refute visible_in_queue?("route_a", other_alpha)
    end

    test "count_queue_messages matches queue+message identity, not JSON envelopes", %{
      flow_slug: flow_slug,
      run_id: run_id,
      unrelated_run: unrelated_run
    } do
      assert {:ok, 2} = Runs.count_queue_messages(TestRepo, flow_slug, run_id)

      hide_messages!("route_a", [@shared_msg_id])

      assert {:ok, 2} =
               Runs.count_queue_messages(TestRepo, flow_slug, run_id, location: :live)

      TestRepo.query!("SELECT pgmq.archive($1::text, $2::bigint)", ["route_b", @shared_msg_id])

      assert {:ok, 1} =
               Runs.count_queue_messages(TestRepo, flow_slug, run_id, location: :archive)

      assert {:ok, 2} =
               Runs.count_queue_messages(TestRepo, flow_slug, unrelated_run, location: :live)
    end

    test "delete removes only the target run's queue messages across routes", %{
      run_id: run_id,
      unrelated_run: unrelated_run
    } do
      assert message_exists?("route_a", @shared_msg_id)
      assert message_exists?("route_b", @shared_msg_id)

      assert :ok = Runs.delete(TestRepo, run_id)

      refute message_exists?("route_a", @shared_msg_id)
      refute message_exists?("route_b", @shared_msg_id)
      assert message_exists?("route_a", task_message_id(unrelated_run, "alpha"))
      assert relational_counts(run_id) == {0, 0, 0}
      assert {:ok, _} = Runs.get(TestRepo, unrelated_run)
    end
  end

  describe "flow deletion and prune" do
    test "delete_flow resolves persisted routes via upstream SQL" do
      flow_slug = "route_delete_flow"
      create_flow(flow_slug)
      ensure_queue!("route_delete_a")
      add_step(flow_slug, "work")

      TestRepo.query!(
        "UPDATE pgflow.steps SET queue_name = $2 WHERE flow_slug = $1",
        [flow_slug, "route_delete_a"]
      )

      _run_id = start_flow_run(flow_slug, %{})

      assert :ok = Flows.delete_flow(TestRepo, flow_slug)
      refute queue_exists?("route_delete_a")
      refute flow_exists?(flow_slug)
    end

    test "prune_data removes old runs without touching unrelated queue messages" do
      flow_slug = "prune_routes"
      create_flow(flow_slug)
      ensure_queue!("prune_route")
      add_step(flow_slug, "work")

      TestRepo.query!(
        "UPDATE pgflow.steps SET queue_name = $2 WHERE flow_slug = $1",
        [flow_slug, "prune_route"]
      )

      old_run = start_flow_run(flow_slug, %{})
      keep_run = start_flow_run(flow_slug, %{})
      ensure_queue!("unrelated_archive")
      old_msg = task_message_id(old_run, "work")
      TestRepo.query!("SELECT pgmq.archive('prune_route', $1::bigint)", [old_msg])
      TestRepo.query!("UPDATE pgmq.a_prune_route SET archived_at = now() - interval '48 hours'")

      %{rows: [[other_msg]]} =
        TestRepo.query!("SELECT pgmq.send('unrelated_archive', '{}'::jsonb)")

      TestRepo.query!("SELECT pgmq.archive('unrelated_archive', $1::bigint)", [other_msg])

      TestRepo.query!(
        "UPDATE pgmq.a_unrelated_archive SET archived_at = now() - interval '48 hours'"
      )

      TestRepo.query!(
        """
        UPDATE pgflow.runs
        SET started_at = now() - interval '48 hours',
            completed_at = now() - interval '48 hours',
            status = 'completed',
            remaining_steps = 0
        WHERE run_id = $1
        """,
        [Ecto.UUID.dump!(old_run)]
      )

      assert {:ok, %{deleted_runs: 1}} = Flows.prune_data(TestRepo, 24, flow_slugs: [flow_slug])

      assert {:error, :not_found} = Runs.get(TestRepo, old_run)
      assert {:ok, _} = Runs.get(TestRepo, keep_run)
      assert message_exists?("prune_route", task_message_id(keep_run, "work"))

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM pgmq.a_prune_route WHERE msg_id = $1", [
                 old_msg
               ])

      assert %{rows: [[1]]} =
               TestRepo.query!(
                 "SELECT count(*) FROM pgmq.a_unrelated_archive WHERE msg_id = $1",
                 [other_msg]
               )
    end
  end

  describe "worker health and notify" do
    test "worker filters use flow identity when queue name differs in case" do
      flow_slug = "MixedCaseWorker"
      queue_name = canonical_queue_name(flow_slug)
      create_flow(flow_slug)
      add_step(flow_slug, "work")

      worker_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa9"

      TestRepo.query!(
        """
        INSERT INTO pgflow.workers
          (worker_id, queue_name, function_name, started_at, last_heartbeat_at)
        VALUES ($1, $2, 'Elixir.Worker.perform/2', NOW(), NOW())
        """,
        [Ecto.UUID.dump!(worker_id), queue_name]
      )

      assert {:ok, true} = Workers.healthy?(TestRepo, flow_slug)
      assert {:ok, true} = Workers.healthy?(TestRepo, queue_name)
      assert {:ok, false} = Workers.healthy?(TestRepo, "unrelated_worker")

      assert {:ok, [%{flow_slug: ^flow_slug}]} =
               Workers.list(TestRepo, flow_slug: flow_slug)
               |> then(fn {:ok, workers} -> {:ok, Enum.map(workers, &Map.from_struct/1)} end)
    end

    @tag :pgmq_notify
    test "notify registers on canonical queue without creating mixed-case queue metadata" do
      if pgmq_notify_available?() do
        flow_slug = "MixedCaseNotify"
        queue_name = canonical_queue_name(flow_slug)
        create_flow(flow_slug)
        add_step(flow_slug, "work")

        refute queue_exists?(flow_slug)

        {:ok, notify_pid} = Notify.start_link(repo: TestRepo, notify_throttle_ms: 0)
        Process.sleep(50)

        assert :ok = Pgmq.enable_notify_insert(TestRepo, queue_name, 0)
        assert :ok = Notify.register_worker(notify_pid, flow_slug, self())

        TestRepo.query!("SELECT pgmq.send($1::text, $2::jsonb)", [queue_name, %{}])
        assert_receive :poll_now, 5_000

        state = :sys.get_state(notify_pid)
        assert Map.has_key?(state.channels, "pgmq.q_#{queue_name}.INSERT")
        refute Map.has_key?(state.channels, "pgmq.q_#{flow_slug}.INSERT")

        GenServer.stop(notify_pid)
      else
        :ok
      end
    end
  end

  defp ensure_queue!(queue_name) do
    TestRepo.query!("SELECT pgmq.create($1::text)", [queue_name])
  end

  defp queue_exists?(queue_name) do
    %{rows: [[exists?]]} =
      TestRepo.query!(
        """
        SELECT to_regclass(format('%I.%I', 'pgmq', pgmq.format_table_name($1::text, 'q'::text))) IS NOT NULL
        """,
        [queue_name]
      )

    exists?
  end

  defp flow_exists?(flow_slug) do
    {:ok, exists?} = Flows.flow_exists?(TestRepo, flow_slug)
    exists?
  end

  defp first_queued_task(run_id) do
    %{rows: [[queue_name, message_id]]} =
      TestRepo.query!(
        """
        SELECT queue_name, message_id
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND message_id IS NOT NULL
        ORDER BY queued_at ASC
        LIMIT 1
        """,
        [Ecto.UUID.dump!(run_id)]
      )

    {queue_name, message_id}
  end

  defp task_message_id(run_id, step_slug) do
    %{rows: [[message_id]]} =
      TestRepo.query!(
        "SELECT message_id FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = $2",
        [Ecto.UUID.dump!(run_id), step_slug]
      )

    message_id
  end

  defp align_message_id!(queue_name, run_id, step_slug, old_id, new_id) do
    TestRepo.query!("DELETE FROM pgmq.q_#{queue_name} WHERE msg_id = $1", [old_id])

    TestRepo.query!(
      """
      INSERT INTO pgmq.q_#{queue_name} (msg_id, vt, message)
      OVERRIDING SYSTEM VALUE
      VALUES ($1, now(), jsonb_build_object('run_id', $2::text))
      """,
      [new_id, run_id]
    )

    TestRepo.query!(
      """
      UPDATE pgflow.step_tasks
      SET message_id = $1, queue_name = $2
      WHERE run_id = $3 AND step_slug = $4
      """,
      [new_id, queue_name, Ecto.UUID.dump!(run_id), step_slug]
    )
  end

  defp hide_messages!(queue_name, msg_ids) do
    Enum.each(msg_ids, fn msg_id ->
      TestRepo.query!(
        "UPDATE pgmq.q_#{queue_name} SET vt = clock_timestamp() + interval '1 hour' WHERE msg_id = $1",
        [msg_id]
      )
    end)
  end

  defp delayed_in_queue?(queue_name, msg_id) do
    table = canonical_queue_table("q", queue_name)

    case TestRepo.query(
           "SELECT EXTRACT(EPOCH FROM (vt - clock_timestamp())) FROM #{table} WHERE msg_id = $1",
           [msg_id]
         ) do
      {:ok, %{rows: [[offset]]}} ->
        offset = if is_struct(offset, Decimal), do: Decimal.to_float(offset), else: offset
        is_number(offset) and offset > 20

      _ ->
        false
    end
  end

  defp visible_in_queue?(queue_name, msg_id) do
    table = canonical_queue_table("q", queue_name)

    %{rows: [[visible]]} =
      TestRepo.query!(
        "SELECT vt <= clock_timestamp() FROM #{table} WHERE msg_id = $1",
        [msg_id]
      )

    visible
  end

  defp message_exists?(queue_name, msg_id) do
    table = canonical_queue_table("q", queue_name)

    %{rows: [[count]]} =
      TestRepo.query!("SELECT count(*) FROM #{table} WHERE msg_id = $1", [msg_id])

    count == 1
  end

  defp canonical_queue_table(prefix, queue_name) do
    %{rows: [[quoted]]} =
      TestRepo.query!(
        "SELECT quote_ident(pgmq.format_table_name($1::text, $2::text))",
        [queue_name, prefix]
      )

    ~s("pgmq".#{quoted})
  end

  defp relational_counts(run_id) do
    %{rows: [[runs, tasks, states]]} =
      TestRepo.query!(
        """
        SELECT
          (SELECT count(*) FROM pgflow.runs WHERE run_id = $1),
          (SELECT count(*) FROM pgflow.step_tasks WHERE run_id = $1),
          (SELECT count(*) FROM pgflow.step_states WHERE run_id = $1)
        """,
        [Ecto.UUID.dump!(run_id)]
      )

    {runs, tasks, states}
  end

  defp pgmq_notify_available? do
    case TestRepo.query(
           "SELECT 1 FROM pg_proc WHERE proname = 'enable_notify_insert' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')"
         ) do
      {:ok, %{num_rows: n}} when n > 0 -> true
      _ -> false
    end
  end
end
