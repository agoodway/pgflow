defmodule PgFlow.Version do
  @moduledoc """
  Runtime identity stamped onto `pgflow.workers.pgflow_version`.

  Upstream TypeScript workers write their package version. The `elixir-`
  prefix lets operators tell the two runtimes apart in one shared database.
  """

  @doc """
  Returns the worker stamp for this Elixir release.

  ## Examples

      iex> PgFlow.Version.worker_stamp()
      "elixir-" <> to_string(Application.spec(:pgflow, :vsn))

  """
  @spec worker_stamp() :: String.t()
  def worker_stamp, do: "elixir-" <> to_string(Application.spec(:pgflow, :vsn))
end
