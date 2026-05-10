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

  describe "ensure_dashboard_dependencies!/0" do
    test "returns :ok when all required dashboard deps are loadable" do
      # In the pgflow test environment phoenix_live_view, live_filter, and tz
      # are present as deps so the real dependency probe must succeed.
      assert :ok = Router.ensure_dashboard_dependencies!()
    end
  end

  describe "missing_dependencies/1" do
    test "returns [] when every module loads" do
      assert Router.missing_dependencies([
               {:elixir, Kernel},
               {:elixir, Enum}
             ]) == []
    end

    test "returns the app names whose modules are not loadable" do
      assert Router.missing_dependencies([
               {:elixir, Kernel},
               {:nonexistent_dep, PgFlowDashboard.NotARealDependencyZ},
               {:other_missing, PgFlowDashboard.AlsoNotRealZ}
             ]) == [:nonexistent_dep, :other_missing]
    end
  end

  describe "check_dashboard_dependencies!/1" do
    test "returns :ok when no deps are missing" do
      assert :ok = Router.check_dashboard_dependencies!([{:elixir, Kernel}])
    end

    test "raises ArgumentError listing missing deps" do
      error =
        assert_raise ArgumentError, fn ->
          Router.check_dashboard_dependencies!([
            {:phoenix_live_view, Phoenix.LiveView},
            {:made_up_dep, PgFlowDashboard.MadeUpDependencyZ}
          ])
        end

      assert error.message =~ "PgFlowDashboard requires :made_up_dep"
      assert error.message =~ "before mounting pgflow_dashboard/2"
    end
  end
end
