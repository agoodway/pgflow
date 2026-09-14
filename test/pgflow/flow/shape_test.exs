defmodule PgFlow.Flow.ShapeTest do
  use ExUnit.Case, async: true

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Flow.{Definition, Shape, Step}
  alias PgFlow.TestRepo

  @cases_path Path.expand("../../support/upstream/shape_cases.json", __DIR__)
  @cases Jason.decode!(File.read!(@cases_path))

  setup tags do
    if tags[:integration] do
      :ok = Sandbox.checkout(TestRepo)
    end

    :ok
  end

  describe "from_definition/1 golden cases" do
    test "charge step with approval dependency" do
      definition = %Definition{
        slug: :payment_flow,
        module: TestShapeFlow,
        steps: [
          %Step{slug: :charge, depends_on: [:approval]}
        ]
      }

      assert Shape.from_definition(definition) == %{
               "steps" => [
                 %{
                   "slug" => "charge",
                   "stepType" => "single",
                   "dependencies" => ["approval"],
                   "whenUnmet" => "skip",
                   "whenExhausted" => "fail",
                   "requiredInputPattern" => %{"defined" => false},
                   "forbiddenInputPattern" => %{"defined" => false}
                 }
               ]
             }
    end

    for %{"id" => id, "definition" => definition_json, "shape" => expected_shape} <-
          @cases["cases"] do
      test "fixture #{id}" do
        definition = build_definition(unquote(Macro.escape(definition_json)))

        assert Shape.from_definition(definition) == unquote(Macro.escape(expected_shape))
      end
    end

    test "flow_type and cron metadata stay outside the upstream shape" do
      definition = %Definition{
        slug: :cron_flow,
        module: TestShapeFlow,
        flow_type: :job,
        opts: [schedule: "@hourly", input: %{"key" => "value"}],
        steps: [%Step{slug: :work}]
      }

      shape = Shape.from_definition(definition)

      refute Map.has_key?(shape, "flowType")
      refute Map.has_key?(shape, "flow_type")
      refute Map.has_key?(shape, "cron")
      refute Map.has_key?(shape, "schedule")
    end
  end

  describe "SQL _compare_flow_shapes/2" do
    @tag :integration
    test "structural changes produce differences" do
      left = shape_for_case("charge_after_approval")

      right =
        put_in(left, ["steps", Access.at(0), "stepType"], "map")

      assert compare_shapes(left, right) != []
    end

    @tag :integration
    test "runtime option changes do not produce differences" do
      left = shape_for_case("runtime_options")
      right = apply_options_override(left, @cases["comparison"]["options_pair"]["right_override"])

      assert compare_shapes(left, right) == []
    end
  end

  defp shape_for_case(id) do
    case = Enum.find(@cases["cases"], &(&1["id"] == id))
    definition = build_definition(case["definition"])
    Shape.from_definition(definition)
  end

  defp apply_options_override(shape, %{"options" => flow_opts, "steps" => step_overrides}) do
    shape
    |> Map.put("options", flow_opts)
    |> update_in(["steps"], fn steps ->
      Enum.zip(steps, step_overrides)
      |> Enum.map(fn {step, override} ->
        Map.put(step, "options", override["options"])
      end)
    end)
  end

  defp compare_shapes(left, right) do
    %{rows: [[differences]]} =
      TestRepo.query!(
        "SELECT pgflow._compare_flow_shapes($1::jsonb, $2::jsonb)",
        [left, right]
      )

    differences || []
  end

  defp build_definition(%{"slug" => slug} = raw) do
    %Definition{
      slug: String.to_atom(slug),
      module: TestShapeFlow,
      flow_type: flow_type(raw["flow_type"]),
      opts: build_flow_opts(raw["opts"] || []),
      steps: Enum.map(raw["steps"], &build_step/1)
    }
  end

  defp flow_type("job"), do: :job
  defp flow_type(_), do: :flow

  defp build_flow_opts(entries) do
    Enum.map(entries, fn
      %{"key" => "max_attempts", "value" => value} -> {:max_attempts, value}
      %{"key" => "base_delay", "value" => value} -> {:base_delay, value}
      %{"key" => "timeout", "value" => value} -> {:timeout, value}
      %{"key" => key, "value" => value} -> {String.to_existing_atom(key), value}
    end)
  end

  defp build_step(raw) do
    opts =
      raw
      |> Map.drop(["slug", "depends_on", "step_type", "if_defined", "if_not_defined"])
      |> Enum.flat_map(&step_opt_pair/1)

    step =
      Step.from_tuple(
        {String.to_atom(raw["slug"]),
         [
           step_type: step_type(raw["step_type"]),
           depends_on: Enum.map(raw["depends_on"] || [], &String.to_atom/1)
         ] ++ opts}
      )

    %{
      step
      | if_defined?: Map.get(raw, "if_defined", Keyword.has_key?(opts, :if)),
        if_not_defined?: Map.get(raw, "if_not_defined", Keyword.has_key?(opts, :if_not))
    }
  end

  defp step_type("map"), do: :map
  defp step_type(_), do: :single

  defp step_opt_pair({"if", value}), do: [if: value]
  defp step_opt_pair({"if_not", value}), do: [if_not: value]
  defp step_opt_pair({"when_unmet", "skip_cascade"}), do: [when_unmet: :skip_cascade]
  defp step_opt_pair({"when_unmet", value}), do: [when_unmet: String.to_atom(value)]
  defp step_opt_pair({"when_exhausted", "skip_cascade"}), do: [when_exhausted: :skip_cascade]
  defp step_opt_pair({"when_exhausted", value}), do: [when_exhausted: String.to_atom(value)]

  defp step_opt_pair({key, value})
       when key in ["max_attempts", "base_delay", "timeout", "start_delay"] do
    [{String.to_atom(key), value}]
  end

  defp step_opt_pair({key, value}), do: [{String.to_atom(key), value}]
end
