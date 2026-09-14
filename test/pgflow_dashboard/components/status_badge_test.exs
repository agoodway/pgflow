defmodule PgFlowDashboard.Components.StatusBadgeTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Components.StatusBadge

  describe "status_badge/1" do
    test "renders Skipped for skipped status" do
      html = render_component(&StatusBadge.status_badge/1, status: "skipped")

      assert html =~ "Skipped"
      assert html =~ "bg-orange-100"
      assert html =~ "text-orange-800"
      assert html =~ "ring-orange-300"
    end

    test "renders Cancelled distinctly from skipped" do
      skipped_html = render_component(&StatusBadge.status_badge/1, status: "skipped")
      cancelled_html = render_component(&StatusBadge.status_badge/1, status: "cancelled")

      assert cancelled_html =~ "Cancelled"
      assert cancelled_html =~ "bg-slate-200"
      assert cancelled_html =~ "ring-slate-400"
      refute cancelled_html =~ "bg-orange-100"
      refute skipped_html =~ "Cancelled"
    end
  end
end
