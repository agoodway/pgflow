defmodule PgFlow.RuntimeFlowManagementTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Client
  alias PgFlow.TestRepo

  @moduletag timeout: 30_000
  @moduletag :integration

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    :persistent_term.put({PgFlow, :repo}, TestRepo)

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})
      Sandbox.mode(TestRepo, :manual)
    end)

    :ok
  end

  # ── ensure_flow ──────────────────────────────────────────────────

  describe "ensure_flow/2" do
    test "creates a new flow (status: compiled)" do
      {:ok, result} =
        Client.ensure_flow("test_runtime_flow",
          max_attempts: 3,
          steps: [
            %{slug: "step_a", deps: []},
            %{slug: "step_b", deps: ["step_a"]}
          ]
        )

      assert result["status"] == "compiled"
      assert result["differences"] == []
    end

    test "same shape returns recompiled on second call" do
      opts = [
        max_attempts: 3,
        steps: [%{slug: "only_step", deps: []}]
      ]

      {:ok, first} = Client.ensure_flow("test_idempotent", opts)
      assert first["status"] == "compiled"

      {:ok, second} = Client.ensure_flow("test_idempotent", opts)
      assert second["status"] == "recompiled"
    end

    test "changed shape returns recompiled" do
      {:ok, _} =
        Client.ensure_flow("test_changed",
          steps: [%{slug: "old_step", deps: []}]
        )

      {:ok, result} =
        Client.ensure_flow("test_changed",
          steps: [
            %{slug: "new_step_a", deps: []},
            %{slug: "new_step_b", deps: ["new_step_a"]}
          ]
        )

      assert result["status"] == "recompiled"
    end

    test "created flow can be started with start_flow" do
      {:ok, _} =
        Client.ensure_flow("test_startable",
          steps: [%{slug: "process", deps: []}]
        )

      {:ok, run_id} = Client.start_flow("test_startable", %{"key" => "value"})
      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end

    test "defaults applied when opts omitted" do
      {:ok, result} =
        Client.ensure_flow("test_defaults",
          steps: [%{slug: "step_one", deps: []}]
        )

      assert result["status"] == "compiled"

      # Verify the flow exists with default options
      {:ok, true} = Client.flow_exists?("test_defaults")
    end

    test "complex multi-dep DAG" do
      {:ok, result} =
        Client.ensure_flow("test_complex_dag",
          max_attempts: 5,
          base_delay: 2,
          timeout: 120,
          steps: [
            %{slug: "fetch", deps: []},
            %{slug: "validate", deps: []},
            %{slug: "transform", deps: ["fetch", "validate"]},
            %{slug: "load", deps: ["transform"]},
            %{slug: "notify", deps: ["transform"]}
          ]
        )

      assert result["status"] == "compiled"

      # Verify it can be started
      {:ok, run_id} = Client.start_flow("test_complex_dag", %{"data" => "test"})
      assert is_binary(run_id)
    end

    test "returns error when steps not provided" do
      assert {:error, ":steps option is required"} = Client.ensure_flow("no_steps", [])
    end
  end

  # ── delete_flow ──────────────────────────────────────────────────

  describe "delete_flow/1" do
    test "deletes existing flow, confirmed via flow_exists?" do
      {:ok, _} =
        Client.ensure_flow("test_delete_me",
          steps: [%{slug: "doomed", deps: []}]
        )

      {:ok, true} = Client.flow_exists?("test_delete_me")

      assert :ok = Client.delete_flow("test_delete_me")

      {:ok, false} = Client.flow_exists?("test_delete_me")
    end

    test "handles nonexistent flow gracefully" do
      assert :ok = Client.delete_flow("nonexistent_flow_xyz")
    end

    test "deletes flow that has existing runs" do
      {:ok, _} =
        Client.ensure_flow("test_delete_with_runs",
          steps: [%{slug: "work", deps: []}]
        )

      # Start a run to create associated data
      {:ok, _run_id} = Client.start_flow("test_delete_with_runs", %{"x" => 1})

      # Should still delete cleanly
      assert :ok = Client.delete_flow("test_delete_with_runs")

      {:ok, false} = Client.flow_exists?("test_delete_with_runs")
    end
  end

  # ── flow_exists? ─────────────────────────────────────────────────

  describe "flow_exists?/1" do
    test "returns true for existing flow" do
      {:ok, _} =
        Client.ensure_flow("test_exists",
          steps: [%{slug: "a", deps: []}]
        )

      assert {:ok, true} = Client.flow_exists?("test_exists")
    end

    test "returns false for nonexistent flow" do
      assert {:ok, false} = Client.flow_exists?("totally_fake_flow")
    end
  end

  # ── facade delegations ──────────────────────────────────────────

  describe "PgFlow facade" do
    test "ensure_flow delegates to Client" do
      {:ok, result} =
        PgFlow.ensure_flow("test_facade_ensure",
          steps: [%{slug: "facade_step", deps: []}]
        )

      assert result["status"] == "compiled"
    end

    test "delete_flow delegates to Client" do
      {:ok, _} =
        PgFlow.ensure_flow("test_facade_delete",
          steps: [%{slug: "temp", deps: []}]
        )

      assert :ok = PgFlow.delete_flow("test_facade_delete")
    end

    test "flow_exists? delegates to Client" do
      assert {:ok, false} = PgFlow.flow_exists?("facade_nonexistent")
    end
  end
end
