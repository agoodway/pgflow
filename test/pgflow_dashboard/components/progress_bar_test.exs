defmodule PgFlowDashboard.Components.ProgressBarTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Components.ProgressBar

  describe "progress_bar/1" do
    test "renders a skipped segment and count when skipped > 0" do
      html =
        render_component(&ProgressBar.progress_bar/1,
          progress: 100,
          completed: 1,
          total: 2,
          failed: 0,
          skipped: 1
        )

      assert html =~ "1 skipped"
      assert html =~ "bg-amber"
    end

    test "renders no skipped segment or count when skipped is 0" do
      html =
        render_component(&ProgressBar.progress_bar/1,
          progress: 50,
          completed: 1,
          total: 2,
          failed: 0,
          skipped: 0
        )

      refute html =~ "skipped"
      refute html =~ "bg-amber"
    end

    test "defaults skipped to 0 when the attribute is omitted" do
      html =
        render_component(&ProgressBar.progress_bar/1,
          progress: 100,
          completed: 2,
          total: 2,
          failed: 0
        )

      refute html =~ "skipped"
    end
  end
end
