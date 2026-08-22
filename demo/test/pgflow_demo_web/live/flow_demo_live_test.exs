defmodule PgflowDemoWeb.FlowDemoLiveTest do
  use PgflowDemoWeb.ConnCase, async: true

  import Phoenix.LiveViewTest

  # These tests exercise handle_info/2 directly by sending synthetic
  # {:pgflow, run_id, event} messages to the LiveView process — the same
  # shape PgFlow.Client broadcasts over PubSub. The handlers don't check
  # the message's run_id against socket.assigns.run_id, so no real flow
  # run or PubSub subscription is required to reach this code path.

  test "clears the error banner when a step_skipped arrives for the step that set it", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, "/")

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:task_failed, %{step_slug: "fetch_article", error: "boom", duration_ms: 12}}}
    )

    html = render(view)
    assert html =~ "Step fetch_article failed"

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:step_skipped, %{step_slug: "fetch_article", skip_reason: "condition_unmet"}}}
    )

    html = render(view)
    refute html =~ "Step fetch_article failed"
  end

  test "leaves the error banner when step_skipped arrives for a different step", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:task_failed, %{step_slug: "fetch_article", error: "boom", duration_ms: 12}}}
    )

    html = render(view)
    assert html =~ "Step fetch_article failed"

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:step_skipped, %{step_slug: "summarize", skip_reason: "condition_unmet"}}}
    )

    html = render(view)
    assert html =~ "Step fetch_article failed"
  end

  test "clears the error banner when the run completes successfully", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:task_failed, %{step_slug: "fetch_article", error: "boom", duration_ms: 12}}}
    )

    html = render(view)
    assert html =~ "Step fetch_article failed"

    send(view.pid, {:pgflow, "fake-run-id", {:run_completed, %{output: %{}}}})

    html = render(view)
    refute html =~ "Step fetch_article failed"
  end

  # A terminal event can arrive twice: once applied from the DB by
  # reconcile_run_state/2 and once from the PubSub message that was already
  # in the mailbox when the reconcile read ran. The guards on the
  # run_completed/run_failed handlers must drop the late duplicate.
  test "a second run_completed after the run is already terminal does not double-log", %{
    conn: conn
  } do
    {:ok, view, _html} = live(conn, "/")

    send(view.pid, {:pgflow, "fake-run-id", {:run_completed, %{output: %{}}}})
    send(view.pid, {:pgflow, "fake-run-id", {:run_completed, %{output: %{}}}})

    html = render(view)
    assert length(String.split(html, "Flow Complete")) - 1 == 1
  end

  test "a late run_failed after the run already completed is ignored", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    send(view.pid, {:pgflow, "fake-run-id", {:run_completed, %{output: %{}}}})
    send(view.pid, {:pgflow, "fake-run-id", {:run_failed, %{error: "too late"}}})

    html = render(view)
    refute html =~ "Flow failed: too late"
    assert html =~ "Completed"
  end

  # Carried finding from Task 3's review: run_failed set :error but left a
  # stale :error_step from an earlier task_failed. A late (or, pre-fix,
  # even a same-run) step_skipped for that stale step would then wrongly
  # clear the run-failure banner via clear_error_banner_for_step/2, since
  # it only compares against error_step.
  test "run_failed clears the stale error_step so a later step_skipped for it doesn't clear the run-failure banner",
       %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:task_failed, %{step_slug: "fetch_article", error: "boom", duration_ms: 12}}}
    )

    html = render(view)
    assert html =~ "Step fetch_article failed"

    send(view.pid, {:pgflow, "fake-run-id", {:run_failed, %{error: "unrecoverable"}}})

    html = render(view)
    assert html =~ "Flow failed: unrecoverable"

    send(
      view.pid,
      {:pgflow, "fake-run-id",
       {:step_skipped, %{step_slug: "fetch_article", skip_reason: "condition_unmet"}}}
    )

    html = render(view)
    assert html =~ "Flow failed: unrecoverable"
  end

  test "Approval tab renders a start form without the article URL field", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    html = view |> element("#tab-approval") |> render_click()
    assert html =~ "Start Flow"
    refute html =~ ~s(name="url")
    assert html =~ "create_order" or html =~ "await_approval"
  end

  test "Cron DSL is not rendered on the default Article tab", %{conn: conn} do
    {:ok, _view, html} = live(conn, "/")

    refute html =~ ~s(id="cron-dsl")
    refute html =~ "Scheduled cleanup job"
    assert html =~ ~s(id="tab-cron")
    assert html =~ "Start Flow"
  end

  test "Cron tab shows the scheduled job DSL and hides flow controls", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    html = view |> element("#tab-cron") |> render_click()

    assert html =~ ~s(id="cron-dsl")
    assert html =~ "Scheduled cleanup job"
    assert html =~ "article_flow_cleanup"
    refute html =~ "Start Flow"
    refute html =~ ~s(id="workflow")
    refute html =~ ~s(id="flow-dsl")
    refute html =~ ~s(id="article-form")
  end

  test "switching from Cron back to Article restores the flow UI", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    view |> element("#tab-cron") |> render_click()
    html = view |> element("#tab-article") |> render_click()

    refute html =~ ~s(id="cron-dsl")
    assert html =~ "Start Flow"
    assert html =~ ~s(id="workflow")
  end

  test "Job DSL is not rendered on the default Article tab", %{conn: conn} do
    {:ok, _view, html} = live(conn, "/")

    refute html =~ ~s(id="job-dsl")
    refute html =~ "PgflowDemo.Jobs.SendEmail"
    assert html =~ ~s(id="tab-job")
    assert html =~ "Start Flow"
  end

  test "Job tab shows the SendEmail DSL and hides flow controls", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    html = view |> element("#tab-job") |> render_click()

    assert html =~ ~s(id="job-dsl")
    assert html =~ "PgflowDemo.Jobs.SendEmail"
    assert html =~ "Start Job"
    refute html =~ ~s(id="workflow")
    refute html =~ ~s(id="flow-dsl")
    refute html =~ ~s(id="article-form")
  end

  test "Cron tab still has no Start Job button", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    html = view |> element("#tab-cron") |> render_click()

    refute html =~ "Start Job"
    assert html =~ ~s(id="cron-dsl")
  end

  test "switching from Job back to Article restores the flow UI", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")

    view |> element("#tab-job") |> render_click()
    html = view |> element("#tab-article") |> render_click()

    refute html =~ ~s(id="job-dsl")
    assert html =~ "Start Flow"
    assert html =~ ~s(id="workflow")
  end

  test "task_waiting for await_approval shows Approve and Reject", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")
    view |> element("#tab-approval") |> render_click()

    send(
      view.pid,
      {:pgflow, "fake-run-id", {:task_waiting, %{step_slug: "await_approval", task_index: 0}}}
    )

    html = render(view)
    assert html =~ "Waiting"
    assert has_element?(view, "#approval-approve")
    assert has_element?(view, "#approval-reject")
  end

  test "apply_waiting_task_statuses overlays waiting tasks over started/running steps" do
    steps_config = [
      %{slug: :create_order},
      %{slug: :await_approval},
      %{slug: :charge}
    ]

    steps = %{create_order: :completed, await_approval: :running, charge: :pending}

    task_rows = [
      %{step_slug: "create_order", status: "completed"},
      %{step_slug: "await_approval", status: "waiting"},
      %{step_slug: "charge", status: "queued"}
    ]

    result =
      PgflowDemoWeb.FlowDemoLive.apply_waiting_task_statuses(steps, task_rows, steps_config)

    assert result[:create_order] == :completed
    assert result[:await_approval] == :waiting
    assert result[:charge] == :pending
  end

  test "apply_waiting_task_statuses ignores unknown slugs and non-waiting rows" do
    steps_config = [%{slug: :await_approval}]
    steps = %{await_approval: :running}

    result =
      PgflowDemoWeb.FlowDemoLive.apply_waiting_task_statuses(
        steps,
        [
          %{step_slug: "await_approval", status: "started"},
          %{step_slug: "unknown_step", status: "waiting"}
        ],
        steps_config
      )

    assert result[:await_approval] == :running
    refute Map.has_key?(result, :unknown_step)
  end

  test "task_started after waiting hides Approve and Reject", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/")
    view |> element("#tab-approval") |> render_click()

    send(
      view.pid,
      {:pgflow, "fake-run-id", {:task_waiting, %{step_slug: "await_approval", task_index: 0}}}
    )

    assert has_element?(view, "#approval-approve")

    send(
      view.pid,
      {:pgflow, "fake-run-id", {:task_started, %{step_slug: "await_approval", task_index: 0}}}
    )

    html = render(view)
    refute has_element?(view, "#approval-approve")
    refute has_element?(view, "#approval-reject")
    assert html =~ "Started"
  end

  # Task 4: the demo must not get stuck showing "running" when a run's
  # broadcast (step:skipped / run:completed / run:failed) fires before the
  # LiveView's PubSub subscription exists. `Client.start_flow/2` can emit
  # those events *synchronously*, inside the call, for a root-only skip
  # that resolves without any worker — so by the time start_flow/2 returns
  # a run_id to the caller, the broadcast has already gone out to zero
  # subscribers and is gone for good. No amount of "subscribe immediately"
  # in the caller fixes that, because the run_id (and therefore the topic
  # name) does not exist until start_flow/2 returns.
  #
  # Reproducing that exact race through the public LiveView UI would
  # require wiring a new conditionally-skipping flow into `@flows` /
  # `@flow_modules` (article_flow's root step, fetch_article, has no
  # condition — it always needs a worker), which is out of scope for this
  # fix. Instead, per the task brief, this test exercises the compensating
  # behavior directly: reconcile_run_state/2 (called right after
  # subscribing, in start_selected_flow/3) must pull in a run's current DB
  # state, so a run that finished before the subscription existed is still
  # reflected in the UI instead of leaving it "stuck on running".
  describe "reconcile_run_state/2 (post-subscribe reconciliation)" do
    @describetag :integration

    test "pulls in a run that finished (with a skipped root step) before we could subscribe" do
      {:ok, run_id} = PgFlow.Client.start_flow(:article_flow, %{"url" => "https://example.com/a"})

      # Simulate the DB state a synchronous root skip would already have
      # produced by the time the caller gets to subscribe: the run is
      # terminal, and its root step is skipped — none of which was ever
      # broadcast to a live subscriber.
      run_uuid = Ecto.UUID.dump!(run_id)

      PgflowDemo.Repo.query!(
        "UPDATE pgflow.step_states SET status = 'skipped', remaining_tasks = NULL, skip_reason = 'condition_unmet', skipped_at = now() WHERE run_id = $1 AND step_slug = 'fetch_article'",
        [run_uuid]
      )

      PgflowDemo.Repo.query!(
        "UPDATE pgflow.runs SET status = 'completed', completed_at = now() WHERE run_id = $1",
        [run_uuid]
      )

      socket = fake_article_socket(run_id)

      socket = PgflowDemoWeb.FlowDemoLive.reconcile_run_state(socket, run_id)

      refute socket.assigns.run_status == :running
      assert socket.assigns.run_status == :completed
      assert socket.assigns.steps[:fetch_article] == :skipped
    end

    test "pulls in a run that already failed before we could subscribe" do
      {:ok, run_id} = PgFlow.Client.start_flow(:article_flow, %{"url" => "https://example.com/a"})

      run_uuid = Ecto.UUID.dump!(run_id)

      PgflowDemo.Repo.query!(
        "UPDATE pgflow.step_states SET status = 'failed', error_message = 'boom', failed_at = now() WHERE run_id = $1 AND step_slug = 'fetch_article'",
        [run_uuid]
      )

      PgflowDemo.Repo.query!(
        "UPDATE pgflow.runs SET status = 'failed', failed_at = now() WHERE run_id = $1",
        [run_uuid]
      )

      socket = fake_article_socket(run_id)

      socket = PgflowDemoWeb.FlowDemoLive.reconcile_run_state(socket, run_id)

      refute socket.assigns.run_status == :running
      assert socket.assigns.run_status == :failed
      assert socket.assigns.error =~ "boom"
      # :error_step stays nil here, same as the live run_failed handler
      # (Task 3's carried fix) — a run-level failure banner must not be
      # dismissable by a later step_skipped for any one step, regardless
      # of whether the banner came from a live event or from reconciling
      # a run that had already failed before we could subscribe.
      assert socket.assigns.error_step == nil
      assert socket.assigns.steps[:fetch_article] == :failed
    end

    test "leaves a still-running run alone (but still reflects the root step's real DB status)" do
      {:ok, run_id} = PgFlow.Client.start_flow(:article_flow, %{"url" => "https://example.com/a"})

      socket = fake_article_socket(run_id)

      socket = PgflowDemoWeb.FlowDemoLive.reconcile_run_state(socket, run_id)

      # article_flow's root step (fetch_article) has no deps, so
      # pgflow.start_flow marks it "started" (ready/queued) immediately —
      # reconcile should surface that instead of leaving it at the
      # locally-assumed :pending default.
      assert socket.assigns.run_status == :running
      assert socket.assigns.steps[:fetch_article] == :running
      assert socket.assigns.steps[:publish] == :pending
    end
  end

  # Minimal socket carrying the assigns start_selected_flow/3 sets before
  # calling reconcile_run_state/2, scoped to the article_flow's steps.
  defp fake_article_socket(run_id) do
    steps_config = [
      %{slug: :fetch_article},
      %{slug: :convert_to_markdown},
      %{slug: :summarize},
      %{slug: :extract_keywords},
      %{slug: :publish}
    ]

    %Phoenix.LiveView.Socket{
      assigns: %{
        __changed__: %{},
        run_id: run_id,
        run_status: :running,
        steps_config: steps_config,
        steps: Map.new(steps_config, fn step -> {step.slug, :pending} end),
        step_outputs: %{},
        error: nil,
        error_step: nil,
        duration: nil,
        start_time: System.monotonic_time(:millisecond),
        event_log: []
      }
    }
  end
end
