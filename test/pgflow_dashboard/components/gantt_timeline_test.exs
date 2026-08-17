defmodule PgFlowDashboard.Components.GanttTimelineTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Components.GanttTimeline

  @run_start ~U[2026-01-01 00:00:00.000000Z]
  @run_end ~U[2026-01-01 00:01:00.000000Z]

  @run %{started_at: @run_start, completed_at: @run_end, status: "completed"}

  describe "gantt_timeline/1 - fully completed run" do
    test "renders a completed bar for every step and no skipped markers" do
      step_states = [
        %{
          step_slug: "step_one",
          status: "completed",
          started_at: DateTime.add(@run_start, 1, :second),
          completed_at: DateTime.add(@run_start, 10, :second),
          skipped_at: nil,
          duration_ms: 9_000
        },
        %{
          step_slug: "step_two",
          status: "completed",
          started_at: DateTime.add(@run_start, 10, :second),
          completed_at: DateTime.add(@run_start, 20, :second),
          skipped_at: nil,
          duration_ms: 10_000
        }
      ]

      html =
        render_component(&GanttTimeline.gantt_timeline/1, run: @run, step_states: step_states)

      # Each step gets a completed bar
      assert html =~ "step_one"
      assert html =~ "step_two"
      assert Regex.scan(~r/fill-emerald-500/, html) |> length() >= 2
      assert html =~ "9.0s"
      assert html =~ "10.0s"

      # None of the skipped-specific styling should be present
      refute html =~ "gantt-skip-ghost"
      refute html =~ "fill-amber"
      refute html =~ "Skipped</text>"

      # No pending dashed indicator either (every step ran to completion)
      refute html =~ ~s(stroke-dasharray="4,4")
    end
  end

  describe "gantt_timeline/1 - skipped steps" do
    test "a never-started skipped step renders a ghost marker and skips the pending dashed bar" do
      step_states = [
        %{
          step_slug: "never_started",
          status: "skipped",
          started_at: nil,
          completed_at: nil,
          skipped_at: nil,
          duration_ms: nil
        }
      ]

      html =
        render_component(&GanttTimeline.gantt_timeline/1, run: @run, step_states: step_states)

      assert html =~ "gantt-skip-ghost"
      assert html =~ "Skipped"
    end

    test "a started-then-skipped step's bar ends at skipped_at, not now/completed_at" do
      started_at = DateTime.add(@run_start, 5, :second)
      skipped_at = DateTime.add(started_at, 5, :second)

      step_states = [
        %{
          step_slug: "started_then_skipped",
          status: "skipped",
          started_at: started_at,
          completed_at: nil,
          skipped_at: skipped_at,
          duration_ms: 5_000
        }
      ]

      html =
        render_component(&GanttTimeline.gantt_timeline/1, run: @run, step_states: step_states)

      assert html =~ "fill-amber"
      refute html =~ "gantt-skip-ghost"
    end

    test "legend includes a Skipped entry" do
      html = render_component(&GanttTimeline.gantt_timeline/1, run: @run, step_states: [])

      assert html =~ "Skipped"
    end
  end
end
