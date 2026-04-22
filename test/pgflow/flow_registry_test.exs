defmodule PgFlow.FlowRegistryTest do
  use ExUnit.Case

  alias PgFlow.FlowRegistry
  alias PgFlow.TestFlows.LinearFlow
  alias PgFlow.TestFlows.SimpleFlow

  setup do
    # Clear the ETS table between tests (registry is started by the application)
    :ets.delete_all_objects(:pgflow_flows)
    :ok
  end

  describe "register/1" do
    test "registers a valid flow module" do
      assert :ok = FlowRegistry.register(SimpleFlow)
    end

    test "returns error tuple for module without __pgflow_definition__" do
      assert {:error, {:invalid_flow_module, Enum}} = FlowRegistry.register(Enum)
    end
  end

  describe "register!/1" do
    test "registers a valid flow module" do
      assert :ok = FlowRegistry.register!(SimpleFlow)
    end

    test "raises for module without __pgflow_definition__" do
      assert_raise ArgumentError, ~r/does not implement PgFlow.Flow behaviour/, fn ->
        FlowRegistry.register!(Enum)
      end
    end
  end

  describe "get/1" do
    test "returns {:ok, flow_def} by module after registration" do
      :ok = FlowRegistry.register(SimpleFlow)

      assert {:ok, flow_def} = FlowRegistry.get(SimpleFlow)
      assert flow_def.module == SimpleFlow
    end

    test "returns {:ok, flow_def} by slug atom after registration" do
      :ok = FlowRegistry.register(SimpleFlow)

      assert {:ok, flow_def} = FlowRegistry.get(:simple_flow)
      assert flow_def.module == SimpleFlow
    end

    test "returns {:error, :not_found} for unregistered module" do
      assert {:error, :not_found} = FlowRegistry.get(LinearFlow)
    end

    test "flow_def contains expected keys" do
      :ok = FlowRegistry.register(SimpleFlow)

      {:ok, flow_def} = FlowRegistry.get(SimpleFlow)
      assert Map.has_key?(flow_def, :module)
      assert Map.has_key?(flow_def, :slug)
      assert Map.has_key?(flow_def, :steps)
      assert Map.has_key?(flow_def, :max_attempts)
    end
  end

  describe "list/0" do
    test "returns empty list when nothing registered" do
      assert FlowRegistry.list() == []
    end

    test "returns flow_defs after registering multiple flows" do
      :ok = FlowRegistry.register(SimpleFlow)
      :ok = FlowRegistry.register(LinearFlow)

      flows = FlowRegistry.list()
      assert length(flows) == 2

      modules = Enum.map(flows, & &1.module) |> Enum.sort()
      assert modules == Enum.sort([SimpleFlow, LinearFlow])
    end

    test "does not include slug-keyed entries (only module entries)" do
      :ok = FlowRegistry.register(SimpleFlow)

      flows = FlowRegistry.list()
      # list/0 filters to only module-keyed entries
      assert length(flows) == 1
      assert hd(flows).module == SimpleFlow
    end
  end

  describe "unregister/1" do
    test "removes both module and slug entries" do
      :ok = FlowRegistry.register(SimpleFlow)
      assert {:ok, _} = FlowRegistry.get(SimpleFlow)
      assert {:ok, _} = FlowRegistry.get(:simple_flow)

      :ok = FlowRegistry.unregister(SimpleFlow)

      assert {:error, :not_found} = FlowRegistry.get(SimpleFlow)
      assert {:error, :not_found} = FlowRegistry.get(:simple_flow)
    end

    test "get/1 returns :not_found after unregister" do
      :ok = FlowRegistry.register(LinearFlow)
      :ok = FlowRegistry.unregister(LinearFlow)

      assert {:error, :not_found} = FlowRegistry.get(LinearFlow)
    end

    test "returns :ok for already-unregistered module (idempotent)" do
      assert :ok = FlowRegistry.unregister(SimpleFlow)
    end
  end
end
