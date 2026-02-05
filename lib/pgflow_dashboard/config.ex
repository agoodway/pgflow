defmodule PgFlowDashboard.Config do
  @moduledoc """
  Configuration validation and management for PgFlowDashboard.

  Uses NimbleOptions to validate configuration options passed to the dashboard router.

  ## Required Options

    * `:repo` - The Ecto repository module to use for database queries.
    * `:pubsub` - The Phoenix.PubSub module for real-time updates.

  ## Optional Options

    * `:refresh_interval` - Polling interval in milliseconds. Default: `5_000`.
    * `:time_zone` - Time zone for displaying timestamps. Default: `"UTC"`.
    * `:default_time_range` - Default time range filter. Default: `:last_24h`.
    * `:max_grid_runs` - Maximum runs to show in history grid. Default: `50`.
    * `:query_timeout` - Database query timeout in milliseconds. Default: `10_000`.
    * `:enable_pubsub` - Whether to enable real-time PubSub updates. Default: `true`.
    * `:cache_ttl` - Cache TTL in milliseconds. Default: `5_000`.

  ## Examples

      config = PgFlowDashboard.Config.validate!(
        repo: MyApp.Repo,
        pubsub: MyApp.PubSub,
        refresh_interval: 10_000
      )

  """

  @schema [
    repo: [
      type: :atom,
      required: true,
      doc: "The Ecto repository module to use for database queries"
    ],
    pubsub: [
      type: :atom,
      required: true,
      doc: "The Phoenix.PubSub module for real-time updates"
    ],
    refresh_interval: [
      type: :pos_integer,
      default: 5_000,
      doc: "Polling interval in milliseconds for dashboard updates"
    ],
    time_zone: [
      type: :string,
      default: "UTC",
      doc: "Time zone for displaying timestamps"
    ],
    default_time_range: [
      type: {:in, [:last_hour, :last_24h, :last_7d, :last_30d]},
      default: :last_24h,
      doc: "Default time range filter for queries"
    ],
    max_grid_runs: [
      type: :pos_integer,
      default: 50,
      doc: "Maximum number of runs to display in the history grid"
    ],
    query_timeout: [
      type: :pos_integer,
      default: 10_000,
      doc: "Database query timeout in milliseconds"
    ],
    enable_pubsub: [
      type: :boolean,
      default: true,
      doc: "Whether to enable real-time PubSub updates"
    ],
    cache_ttl: [
      type: :pos_integer,
      default: 5_000,
      doc: "Cache TTL in milliseconds for expensive aggregations"
    ]
  ]

  @doc """
  Validates the given configuration options.

  Raises `ArgumentError` if the configuration is invalid.

  ## Examples

      iex> PgFlowDashboard.Config.validate!(repo: MyApp.Repo, pubsub: MyApp.PubSub)
      [repo: MyApp.Repo, pubsub: MyApp.PubSub, ...]

      iex> PgFlowDashboard.Config.validate!(pubsub: MyApp.PubSub)
      ** (ArgumentError) required :repo option not found

  """
  @spec validate!(keyword()) :: keyword()
  def validate!(opts) when is_list(opts) do
    case NimbleOptions.validate(opts, @schema) do
      {:ok, config} ->
        validate_repo!(config[:repo])
        config

      {:error, error} ->
        raise ArgumentError, "invalid PgFlowDashboard configuration: #{Exception.message(error)}"
    end
  end

  @doc """
  Returns the NimbleOptions schema for PgFlowDashboard configuration.
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
