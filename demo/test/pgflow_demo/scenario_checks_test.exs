defmodule PgflowDemo.ScenarioChecksTest do
  # Each test owns a private (non-shared) sandbox transaction, so check writes
  # roll back and the running PgFlow pollers keep their own connections.
  use PgflowDemo.DataCase, async: true

  alias PgflowDemo.Checks.{SchemaContract, SlugRules, StepModeGuard}
  alias PgflowDemo.{ScenarioChecks, Scenarios}

  @shadow_slug "ts_step_mode_shadow"

  describe "catalogue" do
    test "check scenarios carry a check module and preset expectations" do
      checks = Enum.filter(Scenarios.list(), &(&1.kind == :check))

      assert checks |> Enum.map(& &1.id) |> Enum.sort() ==
               ["schema_contract", "slug_rules", "step_mode_guard"]

      for descriptor <- checks, {_key, preset} <- descriptor.presets do
        assert Code.ensure_loaded?(descriptor.module)
        assert function_exported?(descriptor.module, :run, 1)
        assert map_size(preset.expected) > 0
        refute Map.has_key?(preset.expected, "run_status")
      end
    end

    test "every check preset passes against the demo database" do
      for descriptor <- Scenarios.list(),
          descriptor.kind == :check,
          {preset_key, _preset} <- descriptor.presets do
        assert {:ok, result} = ScenarioChecks.run(descriptor.id, preset_key)
        assert result.pass, "#{descriptor.id}/#{preset_key}: #{result.message}"
      end
    end
  end

  describe "ScenarioChecks.run/3" do
    test "rejects unknown scenarios, presets, and non-check scenarios" do
      assert {:error, :unknown_scenario} = ScenarioChecks.run("missing", "any")
      assert {:error, :unknown_preset} = ScenarioChecks.run("slug_rules", "missing")
      assert {:error, :not_a_check} = ScenarioChecks.run("retry", "succeed_on_third_attempt")
    end

    test "reports the first mismatched key when expectations differ" do
      assert {:ok, result} =
               ScenarioChecks.run("slug_rules", "reserved_run",
                 expected: %{"elixir_rule" => "ok"}
               )

      refute result.pass
      assert result.actual["elixir_rule"] == "reserved"
      assert result.message =~ "elixir_rule"
    end

    test "mismatch_message/2 compares only expected keys" do
      assert ScenarioChecks.mismatch_message(%{"a" => 1}, %{"a" => 1, "b" => 2}) == nil
      assert ScenarioChecks.mismatch_message(%{"a" => 1}, %{}) =~ "a: expected 1, got nil"
    end
  end

  describe "SlugRules.run/1" do
    test "Elixir and the database agree on each broken rule" do
      for {slug, rule} <- [
            {"run", "reserved"},
            {"a__b", "double_underscore"},
            {"_leading", "bad_start"},
            {"trailing_", "bad_end"},
            {"bad-chars", "bad_chars"},
            {"", "empty"}
          ] do
        result = SlugRules.run(%{"slug" => slug})

        assert result["elixir_rule"] == rule
        assert result["db_is_valid_slug"] == false
        assert result["elixir_db_agree"]
        refute result["valid_step_slug"]
        assert is_binary(result["elixir_rule_message"])
      end
    end

    test "a 48-character slug is a valid step slug but not a flow slug" do
      slug = String.duplicate("a", 48)
      result = SlugRules.run(%{"slug" => slug})

      assert result["elixir_rule"] == "ok"
      assert result["db_is_valid_slug"]
      assert result["valid_step_slug"]
      refute result["valid_flow_slug"]
      assert result["elixir_queue_check"] == "queue_name_too_long"
      assert result["db_queue_check"] == "queue_name_too_long"
      assert result["elixir_db_agree"]

      # The rejected pgmq call leaves the sandbox transaction usable.
      assert %{rows: [[1]]} = Repo.query!("SELECT 1")
    end

    test "a 47-character slug fits the flow queue cap" do
      result = SlugRules.run(%{"slug" => String.duplicate("a", 47)})

      assert result["valid_flow_slug"]
      assert result["db_queue_check"] == "ok"
    end

    test "mixed case is valid and lowercases into the queue name" do
      result = SlugRules.run(%{"slug" => "OrderFlow"})

      assert result["valid_flow_slug"]
      assert result["flow_queue_name"] == "orderflow"
      assert result["elixir_db_agree"]
    end
  end

  describe "SchemaContract.run/1" do
    test "reports the 0.17.2 contract, a clean slug audit, and stamped workers" do
      result = SchemaContract.run(%{})

      assert result["upstream_release"] == "0.17.2"
      assert result["bundled_core_version"] == PgFlow.Migration.current_version()
      assert result["bundled_helpers_version"] == PgFlow.HelpersMigration.current_version()
      assert result["schema_check"] == "ok"
      assert result["slug_audit_findings"] == []
      assert result["worker_stamp"] == "elixir-" <> to_string(Application.spec(:pgflow, :vsn))
      assert result["live_elixir_workers"] > 0
      assert result["live_worker_stamps"] == [result["worker_stamp"]]
      assert result["live_workers_stamped"]
    end
  end

  describe "StepModeGuard.run/1" do
    test "provisions the step-mode flow once and refuses Elixir bootstrap every time" do
      Repo.query!("SELECT pgflow.delete_flow_and_data($1::text)", [@shadow_slug])
      other_flows_before = other_flow_rows()

      first = StepModeGuard.run(%{})
      second = StepModeGuard.run(%{})

      assert first["provision_status"] == "compiled"
      assert second["provision_status"] == "verified"

      for result <- [first, second] do
        assert result["bootstrap_result"] == "refused"

        assert result["refusal"] == %{
                 "reason" => "unsupported_queue_mode",
                 "flow_slug" => @shadow_slug,
                 "queue_mode" => "step"
               }

        assert result["queue_mode"] == "step"
        assert result["definition_unchanged"]
        refute result["worker_function_tracked"]
        refute result["registered_with_flow_starter"]

        assert result["step_queues"] == [
                 "ts_step_mode_shadow__fetch_order",
                 "ts_step_mode_shadow__notify"
               ]
      end

      assert other_flow_rows() == other_flows_before

      assert %{rows: [[1]]} =
               Repo.query!("SELECT count(*)::int FROM pgflow.flows WHERE flow_slug = $1", [
                 @shadow_slug
               ])
    end
  end

  defp other_flow_rows do
    %{rows: rows} =
      Repo.query!(
        "SELECT flow_slug, queue_mode FROM pgflow.flows WHERE flow_slug <> $1 ORDER BY flow_slug",
        [@shadow_slug]
      )

    rows
  end
end
