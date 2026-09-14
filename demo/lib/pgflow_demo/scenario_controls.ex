defmodule PgflowDemo.ScenarioControls do
  @moduledoc """
  Test-bed-only recovery controls for demo-owned scenario runs.
  """

  alias PgFlow.Client
  alias PgflowDemo.ScenarioControls.ReleaseRegistry
  alias PgflowDemo.ScenarioRunner

  @doc "Checks whether this owned recovery run has a live handler ready for release."
  @spec release_ready?(String.t(), String.t()) :: boolean()
  def release_ready?(scenario_id, run_id) do
    ensure_enabled() == :ok and scenario_id == "recovery" and
      ScenarioRunner.demo_run?(scenario_id, run_id) and ReleaseRegistry.registered?(run_id)
  end

  @doc """
  Validates ownership and acknowledges a release request for a blocked recovery handler.
  """
  @spec release_handler(String.t(), String.t()) ::
          {:ok, :released} | {:error, :controls_disabled | :foreign_run | :not_found}
  def release_handler(scenario_id, run_id) do
    with :ok <- ensure_enabled(),
         true <- ScenarioRunner.demo_run?(scenario_id, run_id),
         {:ok, _run} <- Client.get_run(run_id),
         :ok <- release_registered_handler(run_id) do
      {:ok, :released}
    else
      false -> {:error, :foreign_run}
      :not_found -> {:error, :not_found}
      {:error, :not_found} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Requests a graceful worker stop for a demo-owned scenario module.
  """
  @spec drain_worker(String.t(), String.t()) ::
          :ok | {:error, :controls_disabled | :foreign_run | :not_found}
  def drain_worker(scenario_id, run_id) do
    with :ok <- ensure_enabled(),
         true <- ScenarioRunner.demo_run?(scenario_id, run_id),
         {:ok, descriptor} <- PgflowDemo.Scenarios.fetch(scenario_id),
         module when not is_nil(module) <- descriptor.module do
      case Client.get_run(run_id) do
        {:ok, _} ->
          PgFlow.stop_worker(module)

        error ->
          error
      end
    else
      false -> {:error, :foreign_run}
      nil -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Restarts the worker for a demo-owned run in the test-bed profile."
  @spec restart_worker(String.t(), String.t()) :: {:ok, pid()} | {:error, term()}
  def restart_worker(scenario_id, run_id) do
    with :ok <- ensure_enabled(),
         true <- ScenarioRunner.demo_run?(scenario_id, run_id),
         {:ok, %{module: module}} when not is_nil(module) <-
           PgflowDemo.Scenarios.fetch(scenario_id) do
      PgFlow.start_worker(module)
    else
      false -> {:error, :foreign_run}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :not_found}
    end
  end

  defp ensure_enabled do
    if Application.get_env(:pgflow_demo, :scenario_controls_enabled, false) do
      :ok
    else
      {:error, :controls_disabled}
    end
  end

  defp release_registered_handler(run_id) do
    case ReleaseRegistry.release(run_id) do
      :ok -> :ok
      :not_found -> :not_found
    end
  end
end
