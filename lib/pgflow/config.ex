defmodule PgFlow.Config do
  @moduledoc """
  Configuration validation and management for PgFlow.

  Uses NimbleOptions to validate configuration options passed to PgFlow.start_link/1.

  ## Options

    * `:repo` (required) - The Ecto repository module to use for database operations.
    * `:flows` (optional) - List of flow modules to automatically register on startup. Default: `[]`.
    * `:worker_name` (optional) - Human-readable name for workers in logs. Default: auto-generated from flow slug.
    * `:max_concurrency` (optional) - Maximum number of parallel tasks per worker. Default: `10`.
    * `:batch_size` (optional) - Number of messages to fetch per poll cycle. Default: `10`.
    * `:poll_interval` (optional) - Interval between polls in milliseconds. Default: `100`.
    * `:visibility_timeout` (optional) - Message reservation timeout in seconds. Default: `2`.
    * `:attach_default_logger` (optional) - Whether to attach the default telemetry logger. Default: `true`.

  ## Examples

      config = PgFlow.Config.validate!(
        repo: MyApp.Repo,
        flows: [MyApp.Flows.ProcessOrder],
        max_concurrency: 20,
        poll_interval: 200
      )

  """

  @schema [
    repo: [
      type: :atom,
      required: true,
      doc: "The Ecto repository module to use for database operations"
    ],
    flows: [
      type: {:list, :atom},
      default: [],
      doc: "List of flow modules to automatically register on startup"
    ],
    worker_name: [
      type: {:or, [:string, nil]},
      default: nil,
      doc: "Human-readable name prefix for workers in logs (default: pgflow-{flow_slug})"
    ],
    max_concurrency: [
      type: :pos_integer,
      default: 10,
      doc: "Maximum number of parallel tasks per worker"
    ],
    batch_size: [
      type: :pos_integer,
      default: 10,
      doc: "Number of messages to fetch per poll cycle"
    ],
    poll_interval: [
      type: :pos_integer,
      default: 100,
      doc: "Interval between polls in milliseconds"
    ],
    visibility_timeout: [
      type: :pos_integer,
      default: 2,
      doc: "Message reservation timeout in seconds"
    ],
    attach_default_logger: [
      type: :boolean,
      default: false,
      doc:
        "Whether to attach the default telemetry logger (disabled by default since PgFlow.Logger handles structured logging)"
    ]
  ]

  @doc """
  Validates the given configuration options.

  Raises `ArgumentError` if the configuration is invalid.

  ## Examples

      iex> PgFlow.Config.validate!(repo: MyApp.Repo)
      [repo: MyApp.Repo, flows: [], max_concurrency: 10, ...]

      iex> PgFlow.Config.validate!(flows: [MyFlow])
      ** (ArgumentError) required :repo option not found

  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts) when is_list(opts) do
    case NimbleOptions.validate(opts, @schema) do
      {:ok, config} ->
        validate_repo!(config[:repo])
        config

      {:error, error} ->
        raise ArgumentError, "invalid PgFlow configuration: #{Exception.message(error)}"
    end
  end

  @doc """
  Returns the NimbleOptions schema for PgFlow configuration.
  """
  @spec schema() :: keyword()
  def schema, do: @schema

  # Validates that the repo module is loaded and implements the Ecto.Repo behaviour
  defp validate_repo!(repo) do
    unless Code.ensure_loaded?(repo) do
      raise ArgumentError, "repo module #{inspect(repo)} is not loaded"
    end

    unless function_exported?(repo, :__adapter__, 0) do
      raise ArgumentError,
            "repo module #{inspect(repo)} does not implement Ecto.Repo behaviour"
    end

    :ok
  end
end
