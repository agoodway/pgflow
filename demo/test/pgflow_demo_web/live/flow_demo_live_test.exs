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
end
