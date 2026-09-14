defmodule PgFlow.Flow.Shape do
  @moduledoc """
  Serializes compiled flow definitions into the upstream camelCase JSON shape
  consumed by `pgflow.ensure_flow_compiled/2`, `_create_flow_from_shape/2`, and
  `_compare_flow_shapes/2`.

  Defaults and option naming live here so callers share one upstream-compatible
  representation. Runtime metadata such as `flow_type` and cron configuration
  is intentionally excluded from the shape.
  """

  alias PgFlow.Flow.{Definition, Step}

  @default_when_unmet "skip"
  @default_when_exhausted "fail"

  @doc """
  Builds the upstream flow shape for a compiled definition.

  Preserves step declaration order, sorts dependency slugs alphabetically,
  converts `:skip_cascade` to `"skip-cascade"`, and wraps input patterns so
  missing and JSON-null conditions remain distinct.
  """
  @spec from_definition(Definition.t()) :: map()
  def from_definition(%Definition{} = definition) do
    steps = Enum.map(definition.steps, &step_shape/1)

    case flow_options(definition.opts) do
      nil -> %{"steps" => steps}
      options -> %{"steps" => steps, "options" => options}
    end
  end

  defp step_shape(%Step{} = step) do
    base = %{
      "slug" => Step.slug_to_string(step.slug),
      "stepType" => Atom.to_string(step.step_type),
      "dependencies" =>
        step.depends_on
        |> Enum.map(&Step.slug_to_string/1)
        |> Enum.sort(),
      "whenUnmet" => when_unmet(step),
      "whenExhausted" => when_exhausted(step),
      "requiredInputPattern" => input_pattern(Map.get(step, :if_defined?, false), step.if),
      "forbiddenInputPattern" =>
        input_pattern(Map.get(step, :if_not_defined?, false), step.if_not)
    }

    case step_options(step) do
      nil -> base
      options -> Map.put(base, "options", options)
    end
  end

  defp when_unmet(%Step{when_unmet: nil}), do: @default_when_unmet
  defp when_unmet(%Step{when_unmet: mode}), do: mode(mode)

  defp when_exhausted(%Step{when_exhausted: nil}), do: @default_when_exhausted
  defp when_exhausted(%Step{when_exhausted: mode}), do: mode(mode)

  defp mode(:skip_cascade), do: "skip-cascade"
  defp mode(value) when value in [:skip, :fail], do: Atom.to_string(value)

  defp input_pattern(false, _value), do: %{"defined" => false}
  defp input_pattern(true, value), do: %{"defined" => true, "value" => json_value(value)}

  defp json_value(value) when is_map(value) do
    Map.new(value, fn
      {key, nested} when is_atom(key) -> {Atom.to_string(key), json_value(nested)}
      {key, nested} -> {key, json_value(nested)}
    end)
  end

  defp json_value(value) when is_list(value), do: Enum.map(value, &json_value/1)
  defp json_value(value), do: value

  defp flow_options(opts) do
    opts
    |> flow_option_map()
    |> filter_defined_options()
    |> case do
      %{} = empty when map_size(empty) == 0 -> nil
      options -> options
    end
  end

  defp step_options(%Step{} = step) do
    step
    |> step_option_map()
    |> filter_defined_options()
    |> case do
      %{} = empty when map_size(empty) == 0 -> nil
      options -> options
    end
  end

  defp flow_option_map(opts) do
    %{
      "maxAttempts" => Keyword.get(opts, :max_attempts),
      "baseDelay" => Keyword.get(opts, :base_delay),
      "timeout" => Keyword.get(opts, :timeout)
    }
  end

  defp step_option_map(%Step{} = step) do
    %{
      "maxAttempts" => step.max_attempts,
      "baseDelay" => step.base_delay,
      "timeout" => step.timeout,
      "startDelay" => step.start_delay
    }
  end

  defp filter_defined_options(options) do
    options
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end
end
