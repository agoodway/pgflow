defmodule PgflowDemoWeb.ScenariosLiveTest do
  use PgflowDemo.ScenarioCase, async: false

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  @endpoint PgflowDemoWeb.Endpoint
  @moduletag :integration
  @moduletag :integration_sandbox

  setup_all do
    setup_integration_sandbox()
    :ok
  end

  setup do
    {:ok, conn: build_conn()}
  end

  describe "catalogue" do
    test "walkthrough pages render without runnable presets", %{conn: conn} do
      for scenario <- ~w(article observability startup_compilation multi_language external_waits) do
        {:ok, view, _html} = live(conn, "/scenarios/#{scenario}")
        assert has_element?(view, "#scenario-source")
        refute has_element?(view, "#scenario-form")
        refute has_element?(view, "#scenario-run-button")
      end
    end

    test "lists executable and walkthrough scenarios", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios")

      assert has_element?(view, "#scenario-catalogue")
      assert has_element?(view, "a[href='/scenarios/retry']")
      assert has_element?(view, "a[href='/scenarios/article']")
    end
  end

  describe "scenario detail" do
    test "Release waits for handler registration and restores readiness after reload", %{
      conn: conn
    } do
      previous = Application.get_env(:pgflow_demo, :scenario_controls_enabled)
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)
      on_exit(fn -> Application.put_env(:pgflow_demo, :scenario_controls_enabled, previous) end)
      {:ok, run_id} = PgflowDemo.ScenarioRunner.start("recovery", "blocked_handler")
      :ok = Phoenix.PubSub.subscribe(PgflowDemo.PubSub, "scenario:recovery:#{run_id}")
      registry = PgflowDemo.ScenarioControls.ReleaseRegistry

      handler =
        case Map.get(:sys.get_state(registry), run_id) do
          nil ->
            assert_receive {:handler_started, pid}, 20_000
            pid

          pid ->
            pid
        end

      on_exit(fn -> send(handler, :release) end)

      # Reproduce the interval after SQL claim but before registry registration.
      :sys.replace_state(registry, &Map.delete(&1, run_id))
      assert {:ok, loaded} = PgflowDemo.ScenarioRunner.load(run_id)
      assert Enum.any?(loaded.tasks, &(&1.status == "started"))
      {:ok, view, _html} = live(conn, "/scenarios/recovery?run=#{run_id}")
      assert has_element?(view, "#scenario-release-handler[disabled]")
      assert render_click(view, "release_handler", %{}) =~ "not ready"

      :ok = registry.register(run_id, handler)
      refute has_element?(view, "#scenario-release-handler[disabled]")
      {:ok, reloaded, _html} = live(conn, "/scenarios/recovery?run=#{run_id}")
      refute has_element?(reloaded, "#scenario-release-handler[disabled]")
      render_click(reloaded, "release_handler", %{})
      assert has_element?(reloaded, "#scenario-release-handler[disabled]")
      assert has_element?(view, "#scenario-release-handler[disabled]")
      assert await_terminal_run(run_id, timeout_ms: 20_000).status == "completed"
    end

    test "release cannot enable a premature restart while drain is pending", %{conn: conn} do
      previous = Application.get_env(:pgflow_demo, :scenario_controls_enabled)
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)
      on_exit(fn -> Application.put_env(:pgflow_demo, :scenario_controls_enabled, previous) end)
      assert {:ok, run_id} = PgflowDemo.ScenarioRunner.start("recovery", "blocked_handler")
      :ok = Phoenix.PubSub.subscribe(PgflowDemo.PubSub, "scenario:recovery:#{run_id}")
      registry = PgflowDemo.ScenarioControls.ReleaseRegistry

      handler =
        case Map.get(:sys.get_state(registry), run_id) do
          nil ->
            assert_receive {:handler_started, pid}, 20_000
            pid

          pid ->
            pid
        end

      # A controlled release recipient keeps the real handler blocked until the
      # test verifies the drain/restart boundary.
      :ok = registry.register(run_id, self())
      on_exit(fn -> send(handler, :release) end)
      {:ok, view, _html} = live(conn, "/scenarios/recovery?run=#{run_id}")
      render_click(view, "drain_worker", %{})
      assert has_element?(view, "#scenario-restart-worker[disabled]")
      render_click(view, "release_handler", %{})
      assert_receive :release
      assert has_element?(view, "#scenario-restart-worker[disabled]")
      render_click(view, "restart_worker", %{})
      assert has_element?(view, "#scenario-restart-worker[disabled]")
      send(handler, :release)
      render_async(view, 20_000)
      refute has_element?(view, "#scenario-restart-worker[disabled]")
      render_click(view, "restart_worker", %{})
      assert is_pid(PgFlow.WorkerSupervisor.find_worker(PgflowDemo.Flows.RecoveryFlow))
    end

    test "malformed input and controls without a run do not crash", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")
      render_change(view, "validate", %{"preset" => "succeed_on_third_attempt", "input" => "[]"})
      assert has_element?(view, "#scenario-form-errors")
      render_click(view, "release_handler", %{})
      render_click(view, "drain_worker", %{})
      assert has_element?(view, "#scenario-form")
    end

    test "recovery cannot start with controls disabled", %{conn: conn} do
      previous = Application.get_env(:pgflow_demo, :scenario_controls_enabled)
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, false)
      on_exit(fn -> Application.put_env(:pgflow_demo, :scenario_controls_enabled, previous) end)
      {:ok, view, _html} = live(conn, "/scenarios/recovery")
      assert has_element?(view, "#scenario-run-button[disabled]")
      render_submit(view, "run", %{"preset" => "blocked_handler"})
      refute has_element?(view, "#scenario-run[data-run-id]")
    end

    test "shows preset form and source for retry", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")

      assert has_element?(view, "#scenario-input")
      assert has_element?(view, "#scenario-form")
      assert has_element?(view, "#scenario-source")
      assert has_element?(view, "#scenario-source pre")
      assert has_element?(view, "#scenario-source", "defmodule")
      assert has_element?(view, "#scenario-expected")
      assert has_element?(view, "#scenario-run-button")
    end

    test "selecting a preset does not override it with the first preset's input", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/timeout")

      input =
        view
        |> element("#scenario-input-overrides")
        |> render()
        |> Floki.parse_fragment!()
        |> Floki.text()

      render_change(view, "validate", %{"preset" => "slow_path", "input" => input})
      render_submit(view, "run", %{"preset" => "slow_path", "input" => input})

      run_id = run_id_from_view(view)
      assert await_terminal_run(run_id).status == "failed"
      assert has_element?(view, "#scenario-input option[value=slow_path][selected]")

      {:ok, reloaded, _html} = live(conn, "/scenarios/timeout?run=#{run_id}")
      assert has_element?(reloaded, "#scenario-input option[value=slow_path][selected]")
      assert has_element?(reloaded, "#scenario-expected", "failed")
    end

    test "starts a run and shows run inspection UI", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "succeed_on_third_attempt"})

      assert has_element?(view, "#scenario-run")
      assert has_element?(view, "#scenario-dashboard-link")
      assert has_element?(view, "#scenario-tasks")
    end

    test "shows validation errors for out-of-bounds overrides", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")

      view
      |> element("#scenario-form")
      |> render_submit(%{
        "preset" => "succeed_on_third_attempt",
        "input" => Jason.encode!(%{"succeed_on_attempt" => 999})
      })

      assert has_element?(view, "#scenario-form-errors")
      refute has_element?(view, "#scenario-run[data-run-id]")
    end

    test "redirects unknown scenarios to the catalogue", %{conn: conn} do
      assert {:error, {:live_redirect, %{to: "/scenarios"}}} =
               live(conn, "/scenarios/not_a_scenario")
    end
  end

  describe "durable run inspection" do
    test "persists run in URL and reconstructs on reload", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "succeed_on_third_attempt"})

      run_id = run_id_from_view(view)

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, reloaded, _html} = live(conn, "/scenarios/retry?run=#{run_id}")

      assert has_element?(reloaded, "#scenario-run")
      assert render(reloaded) =~ "completed"
    end

    test "ignores pubsub events from a previous scenario run after switching", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "succeed_on_third_attempt"})

      run_id = run_id_from_view(view)

      {:ok, view, _html} = live(conn, "/scenarios/json")

      send(
        view.pid,
        {:pgflow, run_id,
         {:run_completed, %{output: %{"flag" => false}, timestamp: DateTime.utc_now()}}}
      )

      html = render(view)
      refute html =~ run_id
      refute has_element?(view, "#scenario-run[data-run-id='#{run_id}']")
    end

    test "displays retry attempts from persisted task state", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/retry")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "succeed_on_third_attempt"})

      run_id = run_id_from_view(view)

      _run = await_terminal_run(run_id)

      assert render(view) =~ "attempts"
      assert render(view) =~ "3"
    end

    test "displays JSON false and null distinctly", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/json")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "false_and_null"})

      run_id = run_id_from_view(view)

      _run = await_terminal_run(run_id)

      html = render(view)
      assert html =~ "false"
      assert html =~ "null"
      refute html =~ ~s("flag": "false")
    end

    test "displays mixed-case slug and canonical queue identity", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/queue_identity")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "mixed_case"})

      run_id = run_id_from_view(view)

      _run = await_terminal_run(run_id)

      html = render(view)
      assert html =~ "MixedCaseDemo"
    end

    test "displays skipped step siblings", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios/policy_when_unmet_skip")

      view
      |> element("#scenario-form")
      |> render_submit(%{"preset" => "default"})

      run_id = run_id_from_view(view)

      _run = await_terminal_run(run_id)

      html = render(view)
      assert html =~ "skipped"
    end
  end

  describe "test-bed controls" do
    setup do
      previous = Application.get_env(:pgflow_demo, :scenario_controls_enabled, false)

      on_exit(fn ->
        Application.put_env(:pgflow_demo, :scenario_controls_enabled, previous)
      end)

      :ok
    end

    test "hides operational controls when disabled", %{conn: conn} do
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, false)
      {:ok, view, _html} = live(conn, "/scenarios/recovery")
      refute has_element?(view, "#scenario-controls")
    end

    test "shows operational controls when enabled", %{conn: conn} do
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)
      {:ok, view, _html} = live(conn, "/scenarios/recovery")
      assert has_element?(view, "#scenario-controls")
    end
  end

  defp run_id_from_view(view) do
    assert has_element?(view, "#scenario-run[data-run-id]")
    html = render(view)

    case Regex.run(~r/id="scenario-run" data-run-id="([^"]+)"/, html) do
      [_, run_id] -> run_id
      _ -> flunk("expected #scenario-run data-run-id in:\n#{html}")
    end
  end

  describe "navigation" do
    test "article demo remains reachable from scenarios nav", %{conn: conn} do
      {:ok, view, _html} = live(conn, "/scenarios")
      assert has_element?(view, "a[href='/']")
      assert has_element?(view, "a[href='/scenarios/article']")

      {:ok, article_view, html} = live(conn, "/")
      assert html =~ "Article"
      assert has_element?(article_view, "#tab-article")
      assert has_element?(article_view, "#tab-onboarding")
    end
  end
end
