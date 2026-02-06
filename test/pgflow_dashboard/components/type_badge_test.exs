defmodule PgFlowDashboard.Components.TypeBadgeTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias PgFlowDashboard.Components.TypeBadge

  describe "type_badge/1" do
    test "renders badge for job type" do
      html = render_component(&TypeBadge.type_badge/1, type: "job")

      assert html =~ "job"
      assert html =~ "bg-blue-100"
    end

    test "renders nothing for flow type (default)" do
      html = render_component(&TypeBadge.type_badge/1, type: "flow")

      refute html =~ "job"
      refute html =~ "bg-blue-100"
    end

    test "renders nothing when type is nil (defaults to flow)" do
      html = render_component(&TypeBadge.type_badge/1, %{})

      refute html =~ "job"
    end

    test "renders nothing for unknown types" do
      html = render_component(&TypeBadge.type_badge/1, type: "unknown")

      refute html =~ "job"
      refute html =~ "bg-blue-100"
    end
  end
end
