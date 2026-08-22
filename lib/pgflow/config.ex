defmodule PgFlow.Config do
  @moduledoc """
  Configuration validation and management for PgFlow.

  Uses NimbleOptions to validate configuration options passed to PgFlow.start_link/1.

  ## Options

    * `:repo` (required) - The Ecto repository module to use for database operations.
    * `:flows` (optional) - List of flow modules to automatically register on startup. Default: `[]`.
    * `:jobs` (optional) - List of job modules to automatically register on startup. Default: `[]`.
    * `:worker_name` (optional) - Human-readable name for workers in logs. Default: auto-generated from flow slug.
    * `:max_concurrency` (optional) - Maximum number of parallel tasks per worker. Default: `10`.
    * `:batch_size` (optional) - Number of messages to fetch per poll cycle. Default: `10`.
    * `:signal_strategy` (optional) - Signal detection strategy. `:polling` (adaptive backoff) or `:notify` (LISTEN/NOTIFY). Default: `:polling`.
    * `:min_poll_interval` (optional) - Minimum milliseconds between polls (fastest rate). Default: `1000`. Max: `300_000` (5 minutes).
    * `:max_poll_interval` (optional) - Maximum milliseconds between polls (slowest rate during backoff). Default: `5000`. Max: `300_000` (5 minutes).
    * `:notify_fallback_interval` (optional) - Milliseconds between fallback polls when using `:notify` strategy. Default: `30000`. Max: `600_000` (10 minutes).
    * `:notify_throttle_ms` (optional) - Throttle interval for pgmq LISTEN/NOTIFY trigger. Default: `250`.
    * `:recovery_interval` (optional) - Milliseconds between stalled task recovery sweeps. Default: `15_000`.
    * `:waiting_recovery_interval` (optional) - Milliseconds between waiting-task timeout sweeps. Default: `15_000`.
    * `:stale_threshold` (optional) - Buffer in seconds added beyond a task's effective (step or flow) `opt_timeout` before it is considered stalled. Default: `60`.
    * `:attach_default_logger` (optional) - Whether to attach the default telemetry logger. Default: `false`.
    * `:pubsub` (optional) - Phoenix.PubSub module for broadcasting telemetry events to LiveViews. Default: `nil` (disabled).

  ## Interval Constraints

    * `min_poll_interval` must be <= `max_poll_interval`
    * `max_poll_interval` must not exceed 5 minutes (300,000 ms)
    * `notify_fallback_interval` must not exceed 10 minutes (600,000 ms)

  ## Examples

      config = PgFlow.Config.validate!(
        repo: MyApp.Repo,
        flows: [MyApp.Flows.ProcessOrder],
        max_concurrency: 20,
        signal_strategy: :notify
      )

  """

  alias PgFlow.Config.RepoValidator

  # Maximum allowed values for interval configurations
  @max_poll_interval_limit 300_000
  @max_fallback_interval_limit 600_000

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
    jobs: [
      type: {:list, :atom},
      default: [],
      doc: "List of job modules to automatically register on startup"
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
    signal_strategy: [
      type: {:in, [:polling, :notify]},
      default: :polling,
      doc: "Signal detection strategy: :polling (adaptive backoff) or :notify (LISTEN/NOTIFY)"
    ],
    min_poll_interval: [
      type: :pos_integer,
      default: 1_000,
      doc: "Minimum milliseconds between polls (fastest rate when queue is active)"
    ],
    max_poll_interval: [
      type: :pos_integer,
      default: 5_000,
      doc: "Maximum milliseconds between polls (slowest rate during backoff when queue is idle)"
    ],
    notify_fallback_interval: [
      type: :pos_integer,
      default: 30_000,
      doc: "Milliseconds between fallback polls when using :notify strategy (safety net)"
    ],
    notify_throttle_ms: [
      type: :non_neg_integer,
      default: 250,
      doc: "Throttle interval in ms for pgmq LISTEN/NOTIFY trigger (0 = every insert notifies)"
    ],
    recovery_interval: [
      type: :pos_integer,
      default: 15_000,
      doc: "Milliseconds between stalled task recovery sweeps"
    ],
    waiting_recovery_interval: [
      type: :pos_integer,
      default: 15_000,
      doc: "Milliseconds between waiting-task timeout sweeps"
    ],
    stale_threshold: [
      type: :pos_integer,
      default: 60,
      doc:
        "Buffer in seconds beyond a task's effective (step or flow) opt_timeout before it is considered stalled"
    ],
    attach_default_logger: [
      type: :boolean,
      default: false,
      doc:
        "Whether to attach the default telemetry logger (disabled by default since PgFlow.Logger handles structured logging)"
    ],
    pubsub: [
      type: {:or, [:atom, nil]},
      default: nil,
      doc:
        "Phoenix.PubSub module for broadcasting telemetry events. When set, attaches the telemetry-to-PubSub bridge."
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
        RepoValidator.validate_repo!(config[:repo])
        validate_intervals!(config)
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

  # Validates interval configuration constraints
  defp validate_intervals!(config) do
    min_poll = config[:min_poll_interval]
    max_poll = config[:max_poll_interval]
    fallback = config[:notify_fallback_interval]

    if min_poll > max_poll do
      raise ArgumentError,
            "min_poll_interval (#{min_poll}ms) must be <= max_poll_interval (#{max_poll}ms)"
    end

    if max_poll > @max_poll_interval_limit do
      raise ArgumentError,
            "max_poll_interval (#{max_poll}ms) exceeds maximum allowed (#{@max_poll_interval_limit}ms = 5 minutes)"
    end

    if fallback > @max_fallback_interval_limit do
      raise ArgumentError,
            "notify_fallback_interval (#{fallback}ms) exceeds maximum allowed (#{@max_fallback_interval_limit}ms = 10 minutes)"
    end

    :ok
  end
end
