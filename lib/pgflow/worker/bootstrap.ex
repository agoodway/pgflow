defmodule PgFlow.Worker.Bootstrap do
  @moduledoc """
  Compiles and verifies flow definitions before an Elixir worker registers.

  Worker startup always calls upstream `pgflow.ensure_flow_compiled/2`, preserves
  Elixir-specific metadata such as `flow_type`, and only then registers the worker
  function with `pgflow.track_worker_function/2` using the `process` start mode.
  """

  alias Ecto.Adapters.SQL
  alias PgFlow.Flow.{Definition, Shape}
  alias PgFlow.Queries.{Flows, Workers}
  alias PgFlow.SchemaCheck
  require Logger

  @doc """
  Ensures the database definition matches the compiled Elixir definition and
  registers the worker function for monitoring.

  Returns `{:ok, %{queue_name: queue_name, compilation_status: status}}` when
  compilation, verification, or local recompilation succeeds. Shape mismatches
  in production return `{:error, {:flow_shape_mismatch, differences}}`. Missing
  or incompatible schema objects return `{:error, {:schema_incompatible, key}}`.
  """
  @spec prepare(Ecto.Repo.t(), Definition.t()) ::
          {:ok, %{queue_name: String.t(), compilation_status: String.t()}}
          | {:error, term()}
  def prepare(repo, %Definition{} = definition) do
    with :ok <- SchemaCheck.runtime_checks(repo) do
      compile_and_register(repo, definition)
    end
  end

  defp compile_and_register(repo, definition) do
    slug = Definition.slug_to_string(definition)
    shape = Shape.from_definition(definition)
    flow_type = flow_type_string(definition.flow_type)
    function_name = elixir_function_name(definition.module)

    tx_result =
      repo.transaction(fn ->
        with {:ok, %{status: status}} <- Flows.ensure_flow_compiled(repo, slug, shape),
             :ok <- update_flow_type(repo, slug, flow_type),
             {:ok, _} <- Workers.track_worker_function(repo, function_name, "process") do
          %{queue_name: canonical_queue_name(slug), compilation_status: status}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)

    case tx_result do
      {:ok, result} ->
        warn_compilation(repo, definition, result.compilation_status)
        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp warn_compilation(_repo, definition, "recompiled") do
    Logger.warning(
      "Flow #{Definition.slug_to_string(definition)} was locally recompiled; run history was deleted"
    )
  end

  defp warn_compilation(repo, definition, "verified") do
    case Flows.execution_options(repo, Definition.slug_to_string(definition)) do
      {:ok, persisted} ->
        differences =
          Enum.filter(definition.steps, &options_differ?(&1, definition.opts, persisted))

        if differences != [] do
          Logger.warning(
            "Flow #{Definition.slug_to_string(definition)} DSL differs from persisted execution options " <>
              "for #{inspect(Enum.map(differences, & &1.slug))}; startup preserves database values. " <>
              "Use an explicit database migration to update flow/step opt_timeout, opt_max_attempts and opt_base_delay."
          )
        end

      {:error, reason} ->
        Logger.warning("Could not compare persisted execution options: #{inspect(reason)}")
    end
  end

  defp warn_compilation(_repo, _definition, _status), do: :ok

  defp options_differ?(step, flow_opts, persisted) do
    actual = Map.get(persisted, Atom.to_string(step.slug), %{})

    Enum.any?([timeout: 60, max_attempts: 3, base_delay: 1], fn {key, default} ->
      desired = Map.get(step, key) || Keyword.get(flow_opts, key, default)
      Map.get(actual, key) != desired
    end)
  end

  defp update_flow_type(repo, slug, flow_type) do
    case SQL.query(
           repo,
           "UPDATE pgflow.flows SET flow_type = $2::text WHERE flow_slug = $1::text",
           [slug, flow_type]
         ) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp flow_type_string(:job), do: "job"
  defp flow_type_string(_), do: "flow"

  defp elixir_function_name(module) when is_atom(module) do
    "elixir:" <> (module |> Module.split() |> Enum.join("."))
  end

  defp canonical_queue_name(flow_slug), do: String.downcase(flow_slug)
end
