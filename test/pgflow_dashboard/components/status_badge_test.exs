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
  end
end
