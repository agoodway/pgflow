defmodule PgFlowDashboard.Live.RunsLive.ShowTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Live.RunsLive.Show

  @run_start ~U[2026-08-17 09:57:35.000000Z]
  @run_end ~U[2026-08-17 09:57:35.050000Z]

  @run %{
    run_id: "f53bea39-3df7-4020-9047-80ddd19109d0",
    flow_slug: "onboarding_flow",
    status: "completed",
    input: %{"fail_email" => true},
    output: %{},
    started_at: @run_start,
    completed_at: @run_end,
    duration_ms: 50,
    total_steps: 1,
    completed_steps: 0,
    failed_steps: 0,
    skipped_steps: 1,
    progress_percent: 100
  }

  @skipped_state %{
    step_slug: "send_welcome",
    status: "skipped",
    skip_reason: "handler_failed",
    started_at: @run_start,
    completed_at: nil,
    skipped_at: @run_end,
    duration_ms: 50,
    total_tasks: 1,
    completed_tasks: 0,
    failed_tasks: 1,
    deps: ["create_account"]
  }

  test "renders step selection controls with keyboard semantics" do
    html = render_show()

    assert html =~ ~s(phx-keydown="select_step_keydown")
    assert html =~ ~s(<button type="button" id="step-state-send_welcome")
    assert html =~ ~s(tabindex="0")
    assert html =~ ~s(aria-label="Step states")
  end

  test "presents skipped reasons as readable operational context" do
    html = render_show()

    assert html =~ "Skip reason: Handler failed"
    assert html =~ "text-rose-900"
  end

  test "renders run payloads with the reusable JSON viewer" do
    html = render_show()

    assert html =~ ~s(id="run-input-json-code")
    assert html =~ ~s(id="run-input-json-copy")
    assert html =~ "json-token-key"
    assert html =~ ~s(id="run-output-json-code")
    refute html =~ "<pre class=\"text-xs text-slate-700"
  end

  test "does not present run output as the output of a selected step with no tasks" do
    html = render_show(selected_step: "send_welcome", step_tasks: [])

    assert html =~ "No output was recorded for this step"
    refute html =~ ~s(id="run-output-json-code")
  end

  defp render_show(overrides \\ []) do
    assigns =
      Keyword.merge(
        [
          base_path: "/pgflow",
          run: @run,
          time_zone: "UTC",
          selected_step: nil,
          step_tasks: [],
          step_states: [@skipped_state],
          step_state_map: %{"send_welcome" => "skipped"},
          flow_steps: [%{step_slug: "send_welcome", deps: ["create_account"]}]
        ],
        overrides
      )

    render_component(&Show.render/1, assigns)
  end
end
