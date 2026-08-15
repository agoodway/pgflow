defmodule PgFlow.LiveClientTest do
  @moduledoc """
  Tests for PgFlow.LiveClient.

  These are unit-style tests that exercise the LiveClient functions in isolation
  using mock sockets and manually emitted PubSub messages (no real worker needed).

  Integration tests that verify start_flow actually creates DB runs use the
  sandbox and a compiled flow.
  """
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.LiveClient
  alias PgFlow.Schema.Run
  alias PgFlow.TestRepo

  @moduletag timeout: 30_000
  @moduletag :integration

  # ── Flow Module ───────────────────────────────────────────────────

  defmodule LiveClientTestFlow do
    use PgFlow.Flow
    @flow slug: :live_client_test_flow, max_attempts: 3

    step :step_a do
      fn input, _ctx -> %{result: input["value"]} end
    end

    step :step_b, depends_on: [:step_a] do
      fn input, _ctx -> %{final: input["step_a"]["result"] * 2} end
    end
  end

  # ── Setup ─────────────────────────────────────────────────────────

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    :persistent_term.put({PgFlow, :repo}, TestRepo)

    pubsub_name = :"live_client_test_pubsub_#{System.unique_integer([:positive])}"
    start_supervised!({Phoenix.PubSub, name: pubsub_name})

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})
      Sandbox.mode(TestRepo, :manual)
    end)

    compile_flow(LiveClientTestFlow)

    {:ok, pubsub: pubsub_name}
  end

  # ── init/2 ────────────────────────────────────────────────────────

  describe "init/2" do
    test "sets default assign to nil and stores config", %{pubsub: pubsub} do
      socket = build_socket()
      socket = LiveClient.init(socket, pubsub: pubsub)

      assert socket.assigns[:flow_run] == nil
      assert socket.private[:pgflow_live_client].pubsub == pubsub
    end

    test "supports custom assign key via :as option", %{pubsub: pubsub} do
      socket = build_socket()
      socket = LiveClient.init(socket, pubsub: pubsub, as: :my_run)

      assert socket.assigns[:my_run] == nil
      assert socket.assigns[:flow_run] == nil || !Map.has_key?(socket.assigns, :flow_run)
    end
  end

  # ── start_flow/4 ──────────────────────────────────────────────────

  describe "start_flow/4" do
    test "creates run, subscribes, and assigns %Run{}", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 10})

      run = socket.assigns[:flow_run]
      assert %Run{} = run
      assert run.flow_slug == "live_client_test_flow"
      assert run.status == "started"
      assert is_list(run.step_states)
    end

    test "loads step_states via preloading", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 10})

      run = socket.assigns[:flow_run]
      step_slugs = Enum.map(run.step_states, & &1.step_slug) |> Enum.sort()
      assert step_slugs == ["step_a", "step_b"]
    end

    test "returns error for nonexistent flow", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      {:error, _reason, returned_socket} =
        LiveClient.start_flow(socket, "nonexistent_flow", %{})

      assert returned_socket.assigns[:flow_run] == nil
    end

    test "supports :as option", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub, as: :my_run)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1}, as: :my_run)

      assert %Run{} = socket.assigns[:my_run]
    end
  end

  # ── enqueue/4 ─────────────────────────────────────────────────────

  describe "enqueue/4" do
    test "behaves identically to start_flow", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} = LiveClient.enqueue(socket, LiveClientTestFlow, %{"value" => 5})

      assert %Run{} = socket.assigns[:flow_run]
    end
  end

  # ── subscribe/3 ──────────────────────────────────────────────────

  describe "subscribe/3" do
    test "subscribes to existing run and loads snapshot", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      # Create a run in the DB first
      {:ok, run_id} = PgFlow.Client.start_flow(LiveClientTestFlow, %{"value" => 7})

      socket = LiveClient.subscribe(socket, run_id)

      run = socket.assigns[:flow_run]
      assert %Run{} = run
      assert run.run_id == run_id
    end

    test "assigns nil for nonexistent run", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      socket = LiveClient.subscribe(socket, Ecto.UUID.generate())

      assert socket.assigns[:flow_run] == nil
    end
  end

  # ── unsubscribe/2 ─────────────────────────────────────────────────

  describe "unsubscribe/2" do
    test "resets assign to nil", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      assert socket.assigns[:flow_run] != nil

      socket = LiveClient.unsubscribe(socket)
      assert socket.assigns[:flow_run] == nil
    end

    test "is idempotent when no subscription", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      socket = LiveClient.unsubscribe(socket)
      assert socket.assigns[:flow_run] == nil
    end
  end

  # ── handle_info/2 ─────────────────────────────────────────────────

  describe "handle_info/2" do
    test "applies task_started event to matching step", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]

      # step_b starts in "created" status (has dependency on step_a)
      step_b_before = find_step(run, "step_b")
      assert step_b_before.status == "created"

      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id, {:task_started, %{step_slug: "step_b", timestamp: now}}},
          socket
        )

      step_b = find_step(socket.assigns[:flow_run], "step_b")
      assert step_b.status == "started"
      assert step_b.started_at == now
    end

    test "applies task_completed event to matching step", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]

      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:task_completed, %{step_slug: "step_a", output: %{"result" => 42}, timestamp: now}}},
          socket
        )

      step_a = find_step(socket.assigns[:flow_run], "step_a")
      assert step_a.status == "completed"
      assert step_a.output == %{"result" => 42}
      assert step_a.completed_at == now
    end

    test "applies task_failed event to matching step", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]

      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:task_failed, %{step_slug: "step_a", error: "boom", timestamp: now}}},
          socket
        )

      step_a = find_step(socket.assigns[:flow_run], "step_a")
      assert step_a.status == "failed"
      assert step_a.error_message == "boom"
      assert step_a.failed_at == now
    end

    test "applies run_completed event to run struct", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]

      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:run_completed, %{output: %{"final" => "result"}, timestamp: now}}},
          socket
        )

      updated_run = socket.assigns[:flow_run]
      assert updated_run.status == "completed"
      assert updated_run.output == %{"final" => "result"}
      assert updated_run.completed_at == now
    end

    test "applies run_failed event to run struct", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]

      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id, {:run_failed, %{error: "step timed out", timestamp: now}}},
          socket
        )

      updated_run = socket.assigns[:flow_run]
      assert updated_run.status == "failed"
      assert updated_run.failed_at == now
    end

    test "applies step_skipped event to matching step", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]
      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:step_skipped,
            %{step_slug: "step_a", skip_reason: "condition_unmet", timestamp: now}}},
          socket
        )

      step_a = find_step(socket.assigns[:flow_run], "step_a")
      assert step_a.status == "skipped"
      assert step_a.skip_reason == "condition_unmet"
      assert step_a.skipped_at == now
    end

    test "does not overwrite completed with skipped", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]
      now = DateTime.utc_now()

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:task_completed, %{step_slug: "step_a", output: %{}, timestamp: now}}},
          socket
        )

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:step_skipped, %{step_slug: "step_a", skip_reason: "condition_unmet", timestamp: now}}},
          socket
        )

      assert find_step(socket.assigns[:flow_run], "step_a").status == "completed"
    end

    test "ignores events for untracked run_ids", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run_before = socket.assigns[:flow_run]

      # Send event for a different run_id
      socket =
        LiveClient.handle_info(
          {:pgflow, Ecto.UUID.generate(),
           {:run_completed, %{output: %{}, timestamp: DateTime.utc_now()}}},
          socket
        )

      assert socket.assigns[:flow_run] == run_before
    end

    test "ignores unknown event types gracefully", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]

      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id, {:unknown_event, %{}}},
          socket
        )

      # Should not crash, run should be unchanged
      assert socket.assigns[:flow_run].status == run.status
    end
  end

  # ── Status Precedence ─────────────────────────────────────────────

  describe "status precedence" do
    test "status only advances forward (created→started→completed→failed)", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]
      now = DateTime.utc_now()

      # First advance step_a to completed
      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id,
           {:task_completed, %{step_slug: "step_a", output: %{"r" => 1}, timestamp: now}}},
          socket
        )

      assert find_step(socket.assigns[:flow_run], "step_a").status == "completed"

      # Try to go back to started — should be ignored
      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id, {:task_started, %{step_slug: "step_a", timestamp: now}}},
          socket
        )

      assert find_step(socket.assigns[:flow_run], "step_a").status == "completed"
    end

    test "run status cannot go backwards", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)
      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      run = socket.assigns[:flow_run]
      now = DateTime.utc_now()

      # Advance to completed
      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id, {:run_completed, %{output: %{}, timestamp: now}}},
          socket
        )

      assert socket.assigns[:flow_run].status == "completed"

      # run_started should not regress status
      socket =
        LiveClient.handle_info(
          {:pgflow, run.run_id, {:run_started, %{flow_slug: "test", timestamp: now}}},
          socket
        )

      assert socket.assigns[:flow_run].status == "completed"
    end
  end

  # ── Multi-Run via :as ─────────────────────────────────────────────

  describe "multiple runs via :as option" do
    test "tracks two runs independently", %{pubsub: pubsub} do
      socket =
        build_socket()
        |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1}, as: :run_a)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 2}, as: :run_b)

      run_a = socket.assigns[:run_a]
      run_b = socket.assigns[:run_b]

      assert %Run{} = run_a
      assert %Run{} = run_b
      assert run_a.run_id != run_b.run_id
    end

    test "events route to correct assign key", %{pubsub: pubsub} do
      socket =
        build_socket()
        |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1}, as: :run_a)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 2}, as: :run_b)

      run_a = socket.assigns[:run_a]

      now = DateTime.utc_now()

      # Complete run_a only
      socket =
        LiveClient.handle_info(
          {:pgflow, run_a.run_id, {:run_completed, %{output: %{"done" => true}, timestamp: now}}},
          socket
        )

      assert socket.assigns[:run_a].status == "completed"
      assert socket.assigns[:run_b].status == "started"
    end

    test "unsubscribe only affects the specified assign key", %{pubsub: pubsub} do
      socket =
        build_socket()
        |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1}, as: :run_a)

      {:ok, socket} =
        LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 2}, as: :run_b)

      socket = LiveClient.unsubscribe(socket, as: :run_a)

      assert socket.assigns[:run_a] == nil
      assert %Run{} = socket.assigns[:run_b]
    end
  end

  # ── Subscription Cleanup on Re-start ──────────────────────────────

  describe "subscription cleanup" do
    test "starting a new flow on same key cleans up previous subscription", %{pubsub: pubsub} do
      socket = build_socket() |> LiveClient.init(pubsub: pubsub)

      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 1})
      first_run_id = socket.assigns[:flow_run].run_id

      {:ok, socket} = LiveClient.start_flow(socket, LiveClientTestFlow, %{"value" => 2})
      second_run_id = socket.assigns[:flow_run].run_id

      assert first_run_id != second_run_id
      assert socket.assigns[:flow_run].run_id == second_run_id

      # Events for the old run_id should be ignored
      socket =
        LiveClient.handle_info(
          {:pgflow, first_run_id,
           {:run_completed, %{output: %{}, timestamp: DateTime.utc_now()}}},
          socket
        )

      # The flow_run should still be the second run, not completed by old event
      assert socket.assigns[:flow_run].run_id == second_run_id
      assert socket.assigns[:flow_run].status == "started"
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────

  defp build_socket do
    # Build a minimal socket-like struct for testing
    # Phoenix.LiveView.Socket with required fields
    %Phoenix.LiveView.Socket{
      assigns: %{__changed__: %{}},
      private: %{}
    }
  end

  defp find_step(%Run{step_states: states}, step_slug) do
    Enum.find(states, &(&1.step_slug == step_slug))
  end

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    max_attempts = definition.opts[:max_attempts] || 3
    base_delay = definition.opts[:base_delay] || 1
    timeout = definition.opts[:timeout] || 30

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )

    for step <- definition.steps do
      step_slug = Atom.to_string(step.slug)
      deps = Enum.map(step.depends_on, &Atom.to_string/1)
      step_type = Atom.to_string(step.step_type)

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          step_slug,
          deps,
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          step_type
        ]
      )
    end

    flow_slug
  end
end
