defmodule PgFlow.Telemetry.PubSubTest do
  @moduledoc """
  Tests for the PgFlow.Telemetry.PubSub bridge module.

  Verifies:
  - Message format: `{:pgflow, run_id, event_payload}`
  - Topic routing: per-run topic + global topic
  - UUID normalization: 16-byte binary → hyphenated string
  - Opt-in behavior: only attaches when configured
  - Detach cleanup
  """
  use ExUnit.Case, async: false

  alias PgFlow.Telemetry.PubSub, as: TelemetryPubSub

  setup do
    # Start a fresh PubSub for each test to ensure isolation
    pubsub_name = :"test_pubsub_#{System.unique_integer([:positive])}"
    start_supervised!({Phoenix.PubSub, name: pubsub_name})

    # Detach any leftover handler from previous test run
    :telemetry.detach("pgflow-telemetry-pubsub")

    on_exit(fn ->
      :telemetry.detach("pgflow-telemetry-pubsub")
    end)

    {:ok, pubsub: pubsub_name}
  end

  # ── Attach / Detach ───────────────────────────────────────────────

  describe "attach/1 and detach/0" do
    test "attaches handlers successfully", %{pubsub: pubsub} do
      assert :ok = TelemetryPubSub.attach(pubsub: pubsub)
    end

    test "returns error when attaching twice", %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      assert {:error, :already_exists} = TelemetryPubSub.attach(pubsub: pubsub)
    end

    test "detach removes handlers", %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      assert :ok = TelemetryPubSub.detach()
    end

    test "detach returns error when not attached" do
      assert {:error, :not_found} = TelemetryPubSub.detach()
    end
  end

  # ── Task Events ───────────────────────────────────────────────────

  describe "task events" do
    setup %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      :ok
    end

    test "task:start broadcasts to per-run and global topics", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")

      :telemetry.execute(
        [:pgflow, :worker, :task, :start],
        %{},
        %{run_id: run_id, step_slug: "step_a", task_index: 0}
      )

      # Per-run topic
      assert_receive {:pgflow, ^run_id, {:task_started, payload}}
      assert payload.step_slug == "step_a"
      assert payload.task_index == 0
      assert %DateTime{} = payload.timestamp

      # Global topic
      assert_receive {:pgflow, ^run_id, {:task_started, _}}
    end

    test "task:waiting broadcasts to per-run and global topics", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")

      :telemetry.execute(
        [:pgflow, :worker, :task, :waiting],
        %{},
        %{run_id: run_id, step_slug: "await_approval", task_index: 0}
      )

      assert_receive {:pgflow, ^run_id, {:task_waiting, payload}}
      assert payload.step_slug == "await_approval"
      assert payload.task_index == 0
      assert %DateTime{} = payload.timestamp

      assert_receive {:pgflow, ^run_id, {:task_waiting, _}}
    end

    test "task:stop broadcasts with duration and output", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      duration = System.convert_time_unit(150, :millisecond, :native)

      :telemetry.execute(
        [:pgflow, :worker, :task, :stop],
        %{duration: duration},
        %{run_id: run_id, step_slug: "step_b", task_index: 1, output: %{"result" => 42}}
      )

      assert_receive {:pgflow, ^run_id, {:task_completed, payload}}
      assert payload.step_slug == "step_b"
      assert payload.task_index == 1
      assert payload.output == %{"result" => 42}
      assert is_integer(payload.duration_ms)
    end

    test "task:exception broadcasts with error info", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      duration = System.convert_time_unit(50, :millisecond, :native)

      :telemetry.execute(
        [:pgflow, :worker, :task, :exception],
        %{duration: duration},
        %{run_id: run_id, step_slug: "step_c", task_index: 0, reason: "something broke"}
      )

      assert_receive {:pgflow, ^run_id, {:task_failed, payload}}
      assert payload.step_slug == "step_c"
      assert payload.error == "something broke"
      assert is_integer(payload.duration_ms)
    end

    test "task:exception inspects non-string reasons", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      :telemetry.execute(
        [:pgflow, :worker, :task, :exception],
        %{duration: nil},
        %{run_id: run_id, step_slug: "step_d", task_index: 0, reason: {:error, :timeout}}
      )

      assert_receive {:pgflow, ^run_id, {:task_failed, payload}}
      assert payload.error == "{:error, :timeout}"
    end

    test "step:skipped broadcasts to per-run and tasks topics", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")

      :telemetry.execute(
        [:pgflow, :step, :skipped],
        %{system_time: System.system_time()},
        %{
          run_id: run_id,
          flow_slug: "demo",
          step_slug: "premium_only",
          skip_reason: "condition_unmet"
        }
      )

      assert_receive {:pgflow, ^run_id, {:step_skipped, payload}}
      assert payload.step_slug == "premium_only"
      assert payload.skip_reason == "condition_unmet"
      assert %DateTime{} = payload.timestamp

      assert_receive {:pgflow, ^run_id, {:step_skipped, _}}
    end
  end

  # ── Run Events ────────────────────────────────────────────────────

  describe "run events" do
    setup %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      :ok
    end

    test "run:started broadcasts to per-run and global topics", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:runs")

      :telemetry.execute(
        [:pgflow, :run, :started],
        %{system_time: System.system_time()},
        %{run_id: run_id, flow_slug: "my_flow"}
      )

      # Per-run topic
      assert_receive {:pgflow, ^run_id, {:run_started, payload}}
      assert payload.flow_slug == "my_flow"

      # Global topic
      assert_receive {:pgflow, ^run_id, {:run_started, _}}
    end

    test "run:completed broadcasts with output", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:runs")

      :telemetry.execute(
        [:pgflow, :run, :completed],
        %{system_time: System.system_time()},
        %{run_id: run_id, output: %{"result" => "done"}}
      )

      assert_receive {:pgflow, ^run_id, {:run_completed, payload}}
      assert payload.output == %{"result" => "done"}

      # Also on global topic
      assert_receive {:pgflow, ^run_id, {:run_completed, _}}
    end

    test "run:failed broadcasts with error", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:runs")

      :telemetry.execute(
        [:pgflow, :run, :failed],
        %{system_time: System.system_time()},
        %{run_id: run_id, error: "step timed out"}
      )

      assert_receive {:pgflow, ^run_id, {:run_failed, payload}}
      assert payload.error == "step timed out"

      # Also on global topic
      assert_receive {:pgflow, ^run_id, {:run_failed, _}}
    end

    test "run:failed inspects non-string errors", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      :telemetry.execute(
        [:pgflow, :run, :failed],
        %{system_time: System.system_time()},
        %{run_id: run_id, error: {:error, :timeout}}
      )

      assert_receive {:pgflow, ^run_id, {:run_failed, payload}}
      assert payload.error == "{:error, :timeout}"
    end
  end

  # ── UUID Normalization ────────────────────────────────────────────

  describe "UUID normalization" do
    setup %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      :ok
    end

    test "normalizes 16-byte binary UUID to hyphenated string", %{pubsub: pubsub} do
      # Generate a UUID and get both string and binary forms
      string_uuid = Ecto.UUID.generate()
      {:ok, binary_uuid} = Ecto.UUID.dump(string_uuid)

      # Subscribe using the string form (as LiveClient would)
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{string_uuid}")

      # Emit event with binary UUID (as worker telemetry does)
      :telemetry.execute(
        [:pgflow, :run, :started],
        %{system_time: System.system_time()},
        %{run_id: binary_uuid, flow_slug: "test_flow"}
      )

      # Should receive the message with normalized string UUID
      assert_receive {:pgflow, ^string_uuid, {:run_started, _}}
    end

    test "passes through string UUIDs unchanged", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      :telemetry.execute(
        [:pgflow, :run, :started],
        %{system_time: System.system_time()},
        %{run_id: run_id, flow_slug: "test_flow"}
      )

      assert_receive {:pgflow, ^run_id, {:run_started, _}}
    end
  end

  # ── Topic Routing ─────────────────────────────────────────────────

  describe "topic routing" do
    setup %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      :ok
    end

    test "task events go to pgflow:tasks, not pgflow:runs", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:runs")

      :telemetry.execute(
        [:pgflow, :worker, :task, :start],
        %{},
        %{run_id: run_id, step_slug: "s", task_index: 0}
      )

      assert_receive {:pgflow, ^run_id, {:task_started, _}}
      refute_receive {:pgflow, _, _}, 50
    end

    test "run events go to pgflow:runs, not pgflow:tasks", %{pubsub: pubsub} do
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:runs")
      Phoenix.PubSub.subscribe(pubsub, "pgflow:tasks")

      :telemetry.execute(
        [:pgflow, :run, :completed],
        %{system_time: System.system_time()},
        %{run_id: run_id, output: nil}
      )

      assert_receive {:pgflow, ^run_id, {:run_completed, _}}
      refute_receive {:pgflow, _, _}, 50
    end
  end

  # ── Opt-in Behavior ──────────────────────────────────────────────

  describe "opt-in behavior" do
    test "no broadcasts when not attached", %{pubsub: pubsub} do
      # Don't call attach — bridge is not active
      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      :telemetry.execute(
        [:pgflow, :run, :started],
        %{system_time: System.system_time()},
        %{run_id: run_id, flow_slug: "test_flow"}
      )

      refute_receive {:pgflow, _, _}, 100
    end

    test "broadcasts resume after re-attach", %{pubsub: pubsub} do
      :ok = TelemetryPubSub.attach(pubsub: pubsub)
      :ok = TelemetryPubSub.detach()
      :ok = TelemetryPubSub.attach(pubsub: pubsub)

      run_id = Ecto.UUID.generate()
      Phoenix.PubSub.subscribe(pubsub, "pgflow:run:#{run_id}")

      :telemetry.execute(
        [:pgflow, :run, :started],
        %{system_time: System.system_time()},
        %{run_id: run_id, flow_slug: "test_flow"}
      )

      assert_receive {:pgflow, ^run_id, {:run_started, _}}
    end
  end
end
