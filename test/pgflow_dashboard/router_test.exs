defmodule PgFlowDashboard.RouterTest do
  use ExUnit.Case, async: true

  alias PgFlowDashboard.Router

  describe "compute_base_path/2" do
    test "includes outer scope prefix" do
      assert Router.compute_base_path("/admin/pgflow", "/pgflow") == "/admin/pgflow"
    end

    test "includes outer scope prefix with sub-route" do
      assert Router.compute_base_path("/admin/pgflow/flows", "/pgflow") == "/admin/pgflow"
    end

    test "includes outer scope prefix with deep sub-route" do
      assert Router.compute_base_path("/admin/pgflow/runs/abc-123", "/pgflow") == "/admin/pgflow"
    end

    test "works without outer scope" do
      assert Router.compute_base_path("/pgflow", "/pgflow") == "/pgflow"
    end

    test "works without outer scope with sub-route" do
      assert Router.compute_base_path("/pgflow/flows", "/pgflow") == "/pgflow"
    end

    test "handles multi-segment outer scope" do
      assert Router.compute_base_path("/app/admin/pgflow/workers", "/pgflow") ==
               "/app/admin/pgflow"
    end

    test "handles trailing slash on path" do
      assert Router.compute_base_path("/admin/pgflow/flows", "/pgflow/") == "/admin/pgflow"
    end

    test "handles custom dashboard path" do
      assert Router.compute_base_path("/admin/dashboard", "/dashboard") == "/admin/dashboard"
    end

    test "falls back to path when no match found" do
      assert Router.compute_base_path("/something/else", "/pgflow") == "/pgflow"
    end
  end
end
