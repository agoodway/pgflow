defmodule PgflowDemo.ScenariosTest do
  use ExUnit.Case, async: true

  alias PgflowDemo.Flows.MapFlow
  alias PgflowDemo.Scenarios

  @coverage_rows [
    {"sequential_parallel_dag", "parallel"},
    {"article_integration", "article"},
    {"root_dependent_map", "map"},
    {"root_map_variants", "root_map"},
    {"conditional_onboarding", "onboarding"},
    {"conditional_policies", "policy"},
    {"retry_backoff", "retry"},
    {"retry_exhaustion", "exhaustion"},
    {"timeout", "timeout"},
    {"start_delay", "delayed"},
    {"json_values", "json"},
    {"single_step_jobs", "record_job"},
    {"cron_flows_jobs", "scheduled"},
    {"recovery_otp", "recovery"},
    {"queue_identity", "queue_identity"},
    {"observability", "observability"},
    {"startup_compilation", "startup_compilation"},
    {"multi_language", "multi_language"},
    {"external_waits", "external_waits"}
  ]

  describe "list/0" do
    test "returns a finite catalogue" do
      scenarios = Scenarios.list()
      assert is_list(scenarios)
      assert length(scenarios) >= length(@coverage_rows)
      assert length(scenarios) == length(Enum.uniq_by(scenarios, & &1.id))
    end

    test "every coverage row maps to an executable scenario or walkthrough" do
      by_id = Scenarios.list() |> Map.new(&{&1.id, &1})

      for {row, id_prefix} <- @coverage_rows do
        matching =
          Enum.filter(by_id, fn {scenario_id, _} ->
            scenario_id == id_prefix or String.starts_with?(scenario_id, id_prefix <> "_")
          end)

        assert matching != [],
               "coverage row #{row} has no scenario or walkthrough (expected id #{id_prefix}*)"

        assert Enum.all?(matching, fn {_id, descriptor} ->
                 (descriptor.kind in [:executable, :walkthrough] and descriptor.module != nil) or
                   descriptor.kind == :walkthrough
               end)
      end
    end
  end

  describe "fetch/1" do
    test "returns a known scenario" do
      assert {:ok, descriptor} = Scenarios.fetch("retry")
      assert descriptor.id == "retry"
      assert Map.has_key?(descriptor.presets, "succeed_on_third_attempt")
    end

    test "returns error for unknown scenario" do
      assert {:error, :unknown_scenario} = Scenarios.fetch("missing_scenario")
    end
  end

  describe "validate_preset/2" do
    test "map handler bounds direct calls and produces an empty array for zero" do
      handler = MapFlow.__pgflow_handler__(:generate)
      assert handler.(%{"count" => 0}, %{}) == []
      assert handler.(%{"count" => -10}, %{}) == []
      assert handler.(%{"count" => 100}, %{}) == Enum.to_list(1..10)
      assert handler.(%{"count" => "10"}, %{}) == []
    end

    test "rejects negative and non-integer map counts" do
      {:ok, scenario} = Scenarios.fetch("map")
      preset = scenario.presets |> Map.keys() |> hd()

      for count <- [-1, -1_000_000_000, "10", 1.5, nil] do
        assert {:error, :invalid_preset_bounds} =
                 Scenarios.validate_preset("map", preset, %{"count" => count})
      end
    end

    test "rejects invalid retry counts" do
      for attempts <- [-1, 0, "3", nil] do
        assert {:error, :invalid_preset_bounds} =
                 Scenarios.validate_preset("retry", "succeed_on_third_attempt", %{
                   "succeed_on_attempt" => attempts
                 })
      end
    end

    test "rejects process-name overrides" do
      assert {:error, :invalid_preset_bounds} =
               Scenarios.validate_preset("recovery", "blocked_handler", %{
                 "test_name" => "Elixir.PgflowDemo.ScenarioControls.ReleaseRegistry"
               })
    end

    test "rejects unknown presets" do
      assert {:error, :unknown_preset} = Scenarios.validate_preset("retry", "not_a_preset")
    end

    test "rejects out-of-bounds retry counts" do
      assert {:error, :invalid_preset_bounds} =
               Scenarios.validate_preset("retry", "succeed_on_third_attempt", %{
                 "succeed_on_attempt" => 999
               })
    end

    test "rejects unknown override keys" do
      assert {:error, :invalid_preset_bounds} =
               Scenarios.validate_preset("retry", "succeed_on_third_attempt", %{
                 "unexpected" => true
               })
    end
  end
end
