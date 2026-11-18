defmodule PgFlowDashboard.Live.UrlParamsTest do
  use PgFlow.IntegrationCase

  alias PgFlowDashboard.Live.RunsLive
  alias PgFlowDashboard.Live.WorkersLive
  alias Phoenix.LiveView.Socket
  alias Plug.Conn.Query

  @session %{
    "base_path" => "/pgflow",
    "pgflow_dashboard_config" => [
      repo: PgFlow.TestRepo,
      pubsub: nil,
      time_zone: "UTC",
      refresh_interval: 60_000,
      enable_pubsub: false
    ]
  }

  @socket %Socket{private: %{lifecycle: %Phoenix.LiveView.Lifecycle{}, live_temp: %{}}}
  @day "and=(started_at.gte.2026-11-18T00:00:00Z,started_at.lte.2026-11-18T23:59:59Z)"

  describe "RunsLive.Index.handle_params/3" do
    test "default-range redirect preserves every other filter, sort, and pagination param" do
      {:ok, socket} = RunsLive.Index.mount(%{}, @session, @socket)

      for query <- [
            "status=completed",
            "status=completed&flow_slug=in.(a,b)&flow_type=flow&order=started_at.asc&limit=25&offset=50"
          ] do
        original = Query.decode(query)

        assert {:noreply, %Socket{redirected: {:live, :patch, %{to: to}}}} =
                 RunsLive.Index.handle_params(original, "/", socket)

        %URI{path: "/pgflow/runs", query: redirected_query} = URI.parse(to)
        redirected = Query.decode(redirected_query)

        assert redirected["and"] =~ ~r/^\(started_at\.gte\..+,started_at\.lte\..+\)$/

        for {key, value} <- Map.delete(original, "and") do
          assert redirected[key] == value, "dropped #{key} for #{query}"
        end
      end
    end

    test "default-range redirect still applies default pagination when none given" do
      {:ok, socket} = RunsLive.Index.mount(%{}, @session, @socket)

      assert {:noreply, %Socket{redirected: {:live, :patch, %{to: to}}}} =
               RunsLive.Index.handle_params(%{"status" => "completed"}, "/", socket)

      redirected = to |> URI.parse() |> Map.fetch!(:query) |> Query.decode()
      assert %{"limit" => "50", "offset" => "0", "status" => "completed"} = redirected
    end

    test "never crashes on hostile or malformed URL params" do
      {:ok, socket} = RunsLive.Index.mount(%{}, @session, @socket)

      for query <- [
            "status=in.(completed,failed)&#{@day}",
            "status=in.()&#{@day}",
            "flow_slug=in.(a,b)&#{@day}",
            "flow_slug=in.()&#{@day}",
            "flow_type=in.(flow,job)&#{@day}",
            "status=garbage&#{@day}",
            "and=(started_at.gte.garbage,started_at.lte.garbage)",
            "and=(started_at.gte.garbage,started_at.lte.2026-11-18T23:59:59Z)",
            "and=(started_at.gte.2026-11-18,started_at.lte.2026-11-18)",
            "and[]=x",
            "status[]=completed&#{@day}",
            "limit=abc&offset=-5&#{@day}",
            "limit=99999&offset=zzz&#{@day}",
            "order=bogus&#{@day}"
          ] do
        params = Query.decode(query)

        assert {:noreply, %Socket{}} = RunsLive.Index.handle_params(params, "/", socket),
               "crashed on #{query}"
      end
    end
  end

  describe "RunsLive.Show" do
    test "redirects to the runs list for a malformed run id" do
      assert {:ok, %Socket{redirected: {:live, :redirect, %{to: "/pgflow/runs"}}}} =
               RunsLive.Show.mount(%{"id" => "not-a-uuid"}, @session, @socket)
    end

    test "ignores a step param that is not a single slug" do
      create_flow("show_params")
      add_step("show_params", "work")
      run_id = start_flow_run("show_params", %{})

      {:ok, socket} = RunsLive.Show.mount(%{"id" => run_id}, @session, @socket)

      assert {:noreply, %Socket{assigns: %{selected_step: nil}}} =
               RunsLive.Show.handle_params(%{"step" => ["work"]}, "/", socket)

      assert {:noreply, %Socket{assigns: %{selected_step: "work"}}} =
               RunsLive.Show.handle_params(%{"step" => "work"}, "/", socket)
    end
  end

  describe "definition detail pages" do
    test "redirect to their list for an unknown or unusual slug" do
      for {module, key, list_path} <- [
            {PgFlowDashboard.Live.FlowsLive.Show, "slug", "/pgflow/flows"},
            {PgFlowDashboard.Live.JobsLive.Show, "id", "/pgflow/jobs"},
            {PgFlowDashboard.Live.CronsLive.Show, "id", "/pgflow/crons"},
            {PgFlowDashboard.Live.WorkersLive.Show, "id", "/pgflow/workers"}
          ],
          slug <- ["missing_slug", "Weird Slug; DROP", "not-a-uuid"] do
        assert {:ok, %Socket{redirected: {:live, :redirect, %{to: ^list_path}}}} =
                 module.mount(%{key => slug}, @session, @socket)
      end
    end
  end

  describe "WorkersLive.handle_params/3" do
    test "ignores a health param that is not a single value" do
      {:ok, socket} = WorkersLive.mount(%{}, @session, @socket)

      assert {:noreply, %Socket{assigns: %{health_filter: nil}}} =
               WorkersLive.handle_params(%{"health" => ["healthy"]}, "/", socket)

      assert {:noreply, %Socket{assigns: %{health_filter: "healthy"}}} =
               WorkersLive.handle_params(%{"health" => "healthy"}, "/", socket)
    end
  end
end
