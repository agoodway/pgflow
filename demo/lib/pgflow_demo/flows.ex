defmodule PgflowDemo.Flows do
  @moduledoc """
  Context for querying PgFlow run and step data.
  """

  alias PgFlow.Runs
  alias PgFlow.Schema.StepState
  alias PgFlow.Type.JSON
  alias PgflowDemo.Repo

  @doc """
  Fetches the output for a specific step in a flow run.

  Returns the JSON output or nil if not found.
  """
  @spec get_step_output(String.t(), String.t()) :: JSON.value()
  def get_step_output(run_id, step_slug) when is_binary(run_id) and is_binary(step_slug) do
    with {:ok, step_states} <- Runs.list_step_states(Repo, run_id),
         %StepState{output: output} <-
           Enum.find(step_states, &(&1.step_slug == step_slug)) do
      output
    else
      _not_found_or_invalid -> nil
    end
  end

  def get_step_output(_run_id, _step_slug), do: nil
end
