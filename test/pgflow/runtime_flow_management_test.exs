defmodule PgFlow.RuntimeFlowManagementTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Client
  alias PgFlow.Queries.Flows
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

  # ── upsert_flow ──────────────────────────────────────────────────

  describe "upsert_flow/2" do
    test "creates a new flow (status: compiled)" do
      {:ok, result} =
        Client.upsert_flow("test_runtime_flow",
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

      {:ok, first} = Client.upsert_flow("test_idempotent", opts)
      assert first["status"] == "compiled"

      {:ok, second} = Client.upsert_flow("test_idempotent", opts)
      assert second["status"] == "recompiled"
    end

    test "changed shape returns recompiled" do
      {:ok, _} =
        Client.upsert_flow("test_changed",
          steps: [%{slug: "old_step", deps: []}]
        )

      {:ok, result} =
        Client.upsert_flow("test_changed",
          steps: [
            %{slug: "new_step_a", deps: []},
            %{slug: "new_step_b", deps: ["new_step_a"]}
          ]
        )

      assert result["status"] == "recompiled"
    end

    test "created flow can be started with start_flow" do
      {:ok, _} =
        Client.upsert_flow("test_startable",
          steps: [%{slug: "process", deps: []}]
        )

      {:ok, run_id} = Client.start_flow("test_startable", %{"key" => "value"})
      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end

    test "defaults applied when opts omitted" do
      {:ok, result} =
        Client.upsert_flow("test_defaults",
          steps: [%{slug: "step_one", deps: []}]
        )

      assert result["status"] == "compiled"

      # Verify the flow exists with default options
      {:ok, true} = Client.flow_exists?("test_defaults")
    end

    test "complex multi-dep DAG" do
      {:ok, result} =
        Client.upsert_flow("test_complex_dag",
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

    test "applies step-level option overrides" do
      {:ok, _} =
        Client.upsert_flow("test_step_overrides",
          steps: [
            %{slug: "parent", deps: []},
            %{
              slug: "child",
              deps: ["parent"],
              max_attempts: 7,
              timeout: 123,
              start_delay: 4
            }
          ]
        )

      %{rows: [[max_attempts, timeout, start_delay]]} =
        TestRepo.query!(
          "SELECT opt_max_attempts, opt_timeout, opt_start_delay FROM pgflow.steps WHERE flow_slug = $1 AND step_slug = $2",
          ["test_step_overrides", "child"]
        )

      assert max_attempts == 7
      assert timeout == 123
      assert start_delay == 4
    end

    test "returns error when steps not provided" do
      assert {:error, :steps_required} = Client.upsert_flow("no_steps", [])
    end

    test "validates step dependencies refer to declared slugs" do
      assert {:error, {:unknown_dependency, "step_b", "missing"}} =
               Client.upsert_flow("test_invalid_deps",
                 steps: [%{slug: "step_b", deps: ["missing"]}]
               )
    end

    test "validates step type allowlist" do
      assert {:error, {:invalid_step_type, "fanout"}} =
               Client.upsert_flow("test_invalid_step_type",
                 steps: [%{slug: "step_a", step_type: "fanout"}]
               )
    end

    test "validates slug format" do
      assert {:error, {:invalid_slug, "Bad Slug"}} =
               Client.upsert_flow("Bad Slug", steps: [%{slug: "step_a"}])
    end

    test "rejects dashes to match core slug validation" do
      assert {:error, {:invalid_slug, "invalid-flow"}} =
               Client.upsert_flow("invalid-flow", steps: [%{slug: "step_a"}])
    end

    test "rejects reserved slug run" do
      assert {:error, {:invalid_slug, "run"}} =
               Client.upsert_flow("run", steps: [%{slug: "step_a"}])
    end

    test "accepts uppercase slug per core validation" do
      assert {:ok, %{"status" => "compiled"}} =
               Client.upsert_flow("RuntimeFlowV2", steps: [%{slug: "step_a"}])
    end

    test "emits [:pgflow, :flow, :ensured] telemetry" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:pgflow, :flow, :ensured]])

      {:ok, _result} = Client.upsert_flow("test_telemetry_ensured", steps: [%{slug: "step_a"}])

      assert_received {[:pgflow, :flow, :ensured], ^ref, measurements, metadata}
      assert is_integer(measurements.system_time)
      assert metadata.flow_slug == "test_telemetry_ensured"
      assert metadata.status == "compiled"
    end
  end

  # ── delete_flow ──────────────────────────────────────────────────

  describe "delete_flow/1" do
    test "deletes existing flow, confirmed via flow_exists?" do
      {:ok, _} =
        Client.upsert_flow("test_delete_me",
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
        Client.upsert_flow("test_delete_with_runs",
          steps: [%{slug: "work", deps: []}]
        )

      # Start a run to create associated data
      {:ok, _run_id} = Client.start_flow("test_delete_with_runs", %{"x" => 1})

      # Should still delete cleanly
      assert :ok = Client.delete_flow("test_delete_with_runs")

      {:ok, false} = Client.flow_exists?("test_delete_with_runs")
    end

    test "emits [:pgflow, :flow, :deleted] telemetry" do
      {:ok, _} = Client.upsert_flow("test_telemetry_deleted", steps: [%{slug: "cleanup"}])

      ref = :telemetry_test.attach_event_handlers(self(), [[:pgflow, :flow, :deleted]])

      assert :ok = Client.delete_flow("test_telemetry_deleted")

      assert_received {[:pgflow, :flow, :deleted], ^ref, measurements, metadata}
      assert is_integer(measurements.system_time)
      assert metadata.flow_slug == "test_telemetry_deleted"
    end
  end

  # ── flow_exists? ─────────────────────────────────────────────────

  describe "flow_exists?/1" do
    test "returns true for existing flow" do
      {:ok, _} =
        Client.upsert_flow("test_exists",
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
    test "upsert_flow delegates to Client" do
      {:ok, result} =
        PgFlow.upsert_flow("test_facade_ensure",
          steps: [%{slug: "facade_step", deps: []}]
        )

      assert result["status"] == "compiled"
    end

    test "delete_flow delegates to Client" do
      {:ok, _} =
        PgFlow.upsert_flow("test_facade_delete",
          steps: [%{slug: "temp", deps: []}]
        )

      assert :ok = PgFlow.delete_flow("test_facade_delete")
    end

    test "flow_exists? delegates to Client" do
      assert {:ok, false} = PgFlow.flow_exists?("facade_nonexistent")
    end
  end

  describe "query-layer rollback safety" do
    test "upsert_flow rolls back on add_step failure" do
      assert {:error, {:add_step_failed, "bad_step", _reason}} =
               Flows.upsert_flow(
                 TestRepo,
                 "test_rollback_on_step_error",
                 %{},
                 [
                   %{"slug" => "ok_step"},
                   %{"slug" => "bad_step", "step_type" => "invalid"}
                 ]
               )

      assert {:ok, false} = Flows.flow_exists?(TestRepo, "test_rollback_on_step_error")
    end
  end
end
