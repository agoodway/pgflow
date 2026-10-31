defmodule PgflowDemo.ScenarioChecks do
  @moduledoc """
  Runs `:check` catalogue presets.

  A check scenario names a module implementing this behaviour. The module's
  `run/1` receives the preset input and returns a string-keyed, JSON-friendly
  result map. The preset's `expected` map is compared key by key against that
  result, the same way `PgflowDemo.VerifyScenarios` compares run outcomes.
  """

  alias PgflowDemo.Scenarios

  @doc """
  Runs the check for one preset input and returns the observed result map.
  """
  @callback run(input :: map()) :: map()

  @type result :: %{
          scenario_id: String.t(),
          preset: String.t(),
          pass: boolean(),
          expected: map(),
          actual: map(),
          message: String.t() | nil
        }

  @doc """
  Runs a check preset and compares its result with the preset expectation.

  `opts[:expected]` replaces the catalogue expectation (used by tests).
  """
  @spec run(String.t(), String.t(), keyword()) ::
          {:ok, result()} | {:error, :unknown_scenario | :unknown_preset | :not_a_check}
  def run(scenario_id, preset_key, opts \\ []) do
    with {:ok, descriptor} <- Scenarios.fetch(scenario_id),
         :ok <- ensure_check(descriptor),
         {:ok, preset} <- Scenarios.validate_preset(scenario_id, preset_key) do
      expected = Keyword.get(opts, :expected, preset.expected)

      {actual, message} =
        case execute(descriptor.module, preset.input) do
          {:ok, actual} -> {actual, mismatch_message(expected, actual)}
          {:error, message} -> {%{}, message}
        end

      {:ok,
       %{
         scenario_id: scenario_id,
         preset: preset_key,
         pass: is_nil(message),
         expected: expected,
         actual: actual,
         message: message
       }}
    end
  end

  @doc """
  Describes the first expected key whose actual value differs, or nil when all match.
  """
  @spec mismatch_message(map(), map()) :: String.t() | nil
  def mismatch_message(expected, actual) do
    case Enum.find(expected, fn {key, value} -> Map.get(actual, key) != value end) do
      nil ->
        nil

      {key, expected_value} ->
        "#{key}: expected #{inspect(expected_value)}, got #{inspect(Map.get(actual, key))}"
    end
  end

  defp ensure_check(%{kind: :check}), do: :ok
  defp ensure_check(_descriptor), do: {:error, :not_a_check}

  # Reports a crashing check as a failed preset so the verifier and UI can show it.
  defp execute(module, input) do
    {:ok, module.run(input)}
  rescue
    error -> {:error, "check raised #{inspect(error.__struct__)}: #{Exception.message(error)}"}
  end
end
