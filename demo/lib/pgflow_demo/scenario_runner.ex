defmodule PgflowDemo.ScenarioRunner do
  @moduledoc """
  Starts and loads demo scenario runs through public PgFlow APIs.
  """

  alias PgFlow.{Client, Runs}
  alias PgflowDemo.{Repo, Scenarios}

  @type load_result :: %{
          run_id: String.t(),
          scenario_id: String.t(),
          preset: String.t(),
          status: String.t(),
          output: term(),
          step_states: list(),
          tasks: list()
        }

  @doc """
  Starts a scenario preset and returns the run id.
  """
  @spec start(String.t(), String.t(), map()) ::
          {:ok, String.t()}
          | {:error, :unknown_scenario | :unknown_preset | :invalid_preset_bounds | term()}
  def start(scenario_id, preset_key, overrides \\ %{}) do
    with {:ok, descriptor} <- Scenarios.fetch(scenario_id),
         :ok <- ensure_executable(descriptor),
         {:ok, preset} <- Scenarios.validate_preset(scenario_id, preset_key, overrides),
         input <- build_input(scenario_id, preset_key, preset.input) do
      start_module(descriptor.module, input)
    end
  end

  @doc """
  Loads persisted run/task state for a demo-owned run.
  """
  @spec load(String.t()) :: {:ok, load_result()} | {:error, term()}
  def load(run_id) do
    with {:ok, run} <- Runs.get_with_states(Repo, run_id),
         {:ok, tasks} <- Runs.list_run_tasks(Repo, run_id),
         {:ok, scenario_id, preset} <- scenario_metadata(run.input) do
      {:ok,
       %{
         run_id: run.run_id,
         scenario_id: scenario_id,
         preset: preset,
         status: run.status,
         output: run.output,
         step_states: run.step_states,
         tasks: tasks
       }}
    end
  end

  @spec demo_run?(String.t(), String.t()) :: boolean()
  def demo_run?(scenario_id, run_id) do
    case Runs.get(Repo, run_id) do
      {:ok, run} ->
        case scenario_metadata(run.input) do
          {:ok, ^scenario_id, _} -> true
          _ -> false
        end

      _ ->
        false
    end
  end

  defp ensure_executable(%{kind: :executable}), do: :ok
  defp ensure_executable(_), do: {:error, :walkthrough_only}

  defp build_input(scenario_id, preset_key, input) do
    {enqueue, input} = Map.pop(input, "_enqueue", nil)

    input
    |> Map.put("_scenario", %{"id" => scenario_id, "preset" => preset_key})
    |> Map.put("_enqueue", enqueue)
  end

  defp start_module(module, %{"_enqueue" => %{"delay_seconds" => delay}} = input) do
    input = Map.delete(input, "_enqueue")
    Client.enqueue_in(module, input, delay)
  end

  defp start_module(module, input) do
    input = Map.delete(input, "_enqueue")
    Client.start_flow(module, input)
  end

  defp scenario_metadata(%{"_scenario" => %{"id" => id, "preset" => preset}}),
    do: {:ok, id, preset}

  defp scenario_metadata(_), do: {:error, :foreign_run}
end
