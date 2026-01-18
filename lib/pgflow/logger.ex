defmodule PgFlow.Logger do
  @moduledoc """
  Structured logging for PgFlow, aligned with the TypeScript reference implementation.

  This module provides consistent, structured logging across all PgFlow components.
  It supports two output modes:

  - `:fancy` - Colorful, human-readable output for development
  - `:simple` - Structured key=value format for production/log aggregation

  ## Configuration

  Configure the log format in your config:

      config :pgflow, :log_format, :fancy  # or :simple

  The format defaults to `:fancy` in dev/test and `:simple` in prod.

  ## Usage

      alias PgFlow.Logger, as: PgLogger

      # Task lifecycle
      PgLogger.task_started(ctx)
      PgLogger.task_completed(ctx, duration_ms)
      PgLogger.task_failed(ctx, error, retry_info)

      # Worker lifecycle
      PgLogger.startup_banner(startup_ctx)
      PgLogger.polling(worker_name)
      PgLogger.task_count(worker_name, count)
      PgLogger.shutdown(worker_name, :deprecating | :waiting | :stopped)

  ## Metadata

  All log calls include structured metadata that can be filtered and queried
  by log aggregation systems. Use Logger's metadata filtering to control output.
  """

  require Logger

  # Type definitions matching the context we pass around

  @type task_context :: %{
          required(:worker_name) => String.t(),
          required(:worker_id) => String.t(),
          required(:flow_slug) => String.t() | atom(),
          required(:step_slug) => String.t() | atom(),
          required(:run_id) => String.t(),
          optional(:msg_id) => integer(),
          optional(:task_index) => non_neg_integer()
        }

  @type retry_info :: %{
          attempt: pos_integer(),
          max_attempts: pos_integer(),
          delay_seconds: number()
        }

  @type startup_context :: %{
          required(:worker_name) => String.t(),
          required(:worker_id) => String.t(),
          required(:queue_name) => String.t(),
          required(:flows) => [%{flow_slug: String.t() | atom(), status: atom()}]
        }

  @type shutdown_phase :: :deprecating | :waiting | :stopped

  # ============================================================================
  # Task Lifecycle Logging
  # ============================================================================

  @doc """
  Logs when a task execution begins.

  ## Example

      PgFlow.Logger.task_started(%{
        worker_name: "pgflow-orders",
        worker_id: "abc-123",
        flow_slug: "process_order",
        step_slug: "validate",
        run_id: "run-456",
        msg_id: 789,
        task_index: 0
      })

  """
  @spec task_started(task_context()) :: :ok
  def task_started(ctx) do
    metadata = task_metadata(ctx)

    case log_format() do
      :fancy ->
        Logger.debug(
          fn -> fancy_task_started(ctx) end,
          metadata
        )

      :simple ->
        Logger.debug(
          fn -> simple_task_started(ctx) end,
          metadata
        )
    end
  end

  @doc """
  Logs when a task completes successfully.

  ## Example

      PgFlow.Logger.task_completed(ctx, 150)  # completed in 150ms

  """
  @spec task_completed(task_context(), non_neg_integer()) :: :ok
  def task_completed(ctx, duration_ms) do
    metadata = task_metadata(ctx) ++ [duration_ms: duration_ms]

    case log_format() do
      :fancy ->
        Logger.info(
          fn -> fancy_task_completed(ctx, duration_ms) end,
          metadata
        )

      :simple ->
        Logger.info(
          fn -> simple_task_completed(ctx, duration_ms) end,
          metadata
        )
    end
  end

  @doc """
  Logs when a task fails.

  If `retry_info` is provided, includes retry attempt information.

  ## Example

      PgFlow.Logger.task_failed(ctx, "Connection timeout", %{
        attempt: 2,
        max_attempts: 3,
        delay_seconds: 10
      })

  """
  @spec task_failed(task_context(), String.t(), retry_info() | nil) :: :ok
  def task_failed(ctx, error, retry_info \\ nil) do
    metadata =
      task_metadata(ctx) ++
        [error: error] ++
        retry_metadata(retry_info)

    case log_format() do
      :fancy ->
        Logger.warning(
          fn -> fancy_task_failed(ctx, error, retry_info) end,
          metadata
        )

      :simple ->
        Logger.warning(
          fn -> simple_task_failed(ctx, error, retry_info) end,
          metadata
        )
    end
  end

  # ============================================================================
  # Worker Lifecycle Logging
  # ============================================================================

  @doc """
  Logs the worker startup banner with flow compilation status.

  ## Example

      PgFlow.Logger.startup_banner(%{
        worker_name: "pgflow-orders",
        worker_id: "abc-123",
        queue_name: "process_order",
        flows: [
          %{flow_slug: "process_order", status: :compiled}
        ]
      })

  """
  @spec startup_banner(startup_context()) :: :ok
  def startup_banner(ctx) do
    metadata = [
      worker_name: ctx.worker_name,
      worker_id: ctx.worker_id,
      queue_name: ctx.queue_name
    ]

    case log_format() do
      :fancy ->
        Logger.info(fn -> fancy_startup_banner(ctx) end, metadata)

      :simple ->
        log_simple_startup_flows(ctx, metadata)
    end
  end

  defp log_simple_startup_flows(ctx, metadata) do
    Enum.each(ctx.flows, fn flow ->
      flow_metadata = metadata ++ [flow_slug: to_string(flow.flow_slug), status: flow.status]
      Logger.info(fn -> simple_startup_banner(ctx, flow) end, flow_metadata)
    end)

    :ok
  end

  @doc """
  Logs that the worker is polling for messages.
  """
  @spec polling(String.t()) :: :ok
  def polling(worker_name) do
    metadata = [worker_name: worker_name]

    case log_format() do
      :fancy ->
        Logger.debug(
          fn -> "#{worker_name}: Polling..." end,
          metadata
        )

      :simple ->
        Logger.debug(
          fn -> "worker=#{worker_name} status=polling" end,
          metadata
        )
    end
  end

  @doc """
  Logs the number of tasks found after polling.
  """
  @spec task_count(String.t(), non_neg_integer()) :: :ok
  def task_count(worker_name, 0) do
    metadata = [worker_name: worker_name, task_count: 0]

    case log_format() do
      :fancy ->
        Logger.debug(
          fn -> "#{worker_name}: No tasks" end,
          metadata
        )

      :simple ->
        Logger.debug(
          fn -> "worker=#{worker_name} status=no_tasks" end,
          metadata
        )
    end
  end

  def task_count(worker_name, count) when count > 0 do
    metadata = [worker_name: worker_name, task_count: count]

    case log_format() do
      :fancy ->
        Logger.debug(
          fn -> "#{worker_name}: Starting #{count} #{pluralize_task(count)}" end,
          metadata
        )

      :simple ->
        Logger.debug(
          fn -> "worker=#{worker_name} status=starting task_count=#{count}" end,
          metadata
        )
    end
  end

  @doc """
  Logs worker shutdown phases.

  Phases:
  - `:deprecating` - Worker marked for deprecation, stopped accepting tasks
  - `:waiting` - Waiting for in-flight tasks to complete
  - `:stopped` - Worker has stopped gracefully
  """
  @spec shutdown(String.t(), shutdown_phase()) :: :ok
  def shutdown(worker_name, phase) do
    metadata = [worker_name: worker_name, shutdown_phase: phase]

    case log_format() do
      :fancy ->
        Logger.info(
          fn -> fancy_shutdown(worker_name, phase) end,
          metadata
        )

      :simple ->
        Logger.info(
          fn -> "worker=#{worker_name} status=#{phase}" end,
          metadata
        )
    end
  end

  # ============================================================================
  # Run Lifecycle Logging
  # ============================================================================

  @doc """
  Logs when a flow run starts.
  """
  @spec run_started(String.t() | atom(), String.t()) :: :ok
  def run_started(flow_slug, run_id) do
    metadata = [flow_slug: to_string(flow_slug), run_id: run_id, event: :run_started]

    case log_format() do
      :fancy ->
        Logger.info(
          fn -> "Run started: #{flow_slug} (#{short_id(run_id)})" end,
          metadata
        )

      :simple ->
        Logger.info(
          fn -> "flow=#{flow_slug} run_id=#{run_id} status=started" end,
          metadata
        )
    end
  end

  @doc """
  Logs when a flow run completes.
  """
  @spec run_completed(String.t() | atom(), String.t(), non_neg_integer()) :: :ok
  def run_completed(flow_slug, run_id, duration_ms) do
    metadata = [
      flow_slug: to_string(flow_slug),
      run_id: run_id,
      duration_ms: duration_ms,
      event: :run_completed
    ]

    case log_format() do
      :fancy ->
        Logger.info(
          fn -> "Run completed: #{flow_slug} (#{short_id(run_id)}) in #{duration_ms}ms" end,
          metadata
        )

      :simple ->
        Logger.info(
          fn ->
            "flow=#{flow_slug} run_id=#{run_id} status=completed duration_ms=#{duration_ms}"
          end,
          metadata
        )
    end
  end

  @doc """
  Logs when a flow run fails.
  """
  @spec run_failed(String.t() | atom(), String.t(), non_neg_integer(), String.t()) :: :ok
  def run_failed(flow_slug, run_id, duration_ms, error) do
    metadata = [
      flow_slug: to_string(flow_slug),
      run_id: run_id,
      duration_ms: duration_ms,
      error: error,
      event: :run_failed
    ]

    case log_format() do
      :fancy ->
        Logger.error(
          fn ->
            "Run failed: #{flow_slug} (#{short_id(run_id)}) after #{duration_ms}ms - #{error}"
          end,
          metadata
        )

      :simple ->
        Logger.error(
          fn ->
            "flow=#{flow_slug} run_id=#{run_id} status=failed duration_ms=#{duration_ms} error=\"#{escape_quotes(error)}\""
          end,
          metadata
        )
    end
  end

  # ============================================================================
  # Private: Fancy Formatters (Development)
  # ============================================================================

  defp fancy_task_started(ctx) do
    "#{ctx.worker_name}: › #{ctx.flow_slug}/#{ctx.step_slug}[#{ctx[:task_index] || 0}]"
  end

  defp fancy_task_completed(ctx, duration_ms) do
    "#{ctx.worker_name}: ✓ #{ctx.flow_slug}/#{ctx.step_slug}[#{ctx[:task_index] || 0}] #{duration_ms}ms"
  end

  defp fancy_task_failed(ctx, error, nil) do
    """
    #{ctx.worker_name}: ✗ #{ctx.flow_slug}/#{ctx.step_slug}[#{ctx[:task_index] || 0}]
    #{ctx.worker_name}:   #{truncate_error(error)}\
    """
  end

  defp fancy_task_failed(ctx, error, retry_info) do
    if retry_info.attempt < retry_info.max_attempts do
      """
      #{ctx.worker_name}: ✗ #{ctx.flow_slug}/#{ctx.step_slug}[#{ctx[:task_index] || 0}]
      #{ctx.worker_name}:   #{truncate_error(error)}
      #{ctx.worker_name}:   ↻ retry #{retry_info.attempt + 1}/#{retry_info.max_attempts} in #{retry_info.delay_seconds}s\
      """
    else
      """
      #{ctx.worker_name}: ✗ #{ctx.flow_slug}/#{ctx.step_slug}[#{ctx[:task_index] || 0}]
      #{ctx.worker_name}:   #{truncate_error(error)}
      #{ctx.worker_name}:   ✗ max retries exhausted (#{retry_info.max_attempts})\
      """
    end
  end

  defp fancy_startup_banner(ctx) do
    flows_lines =
      Enum.map_join(ctx.flows, "\n", fn flow ->
        icon = if flow.status in [:compiled, :verified, :ready], do: "✓", else: "!"
        "   #{icon} #{flow.flow_slug} (#{flow.status})"
      end)

    """
    ➜ #{ctx.worker_name} [#{short_id(ctx.worker_id)}]
       Queue: #{ctx.queue_name}
       Flows:
    #{flows_lines}\
    """
  end

  defp fancy_shutdown(worker_name, :deprecating) do
    """
    #{worker_name}: ℹ Marked for deprecation
    #{worker_name}:   → Stopped accepting new tasks\
    """
  end

  defp fancy_shutdown(worker_name, :waiting) do
    "#{worker_name}:   → Waiting for in-flight tasks..."
  end

  defp fancy_shutdown(worker_name, :stopped) do
    "#{worker_name}: ✓ Stopped gracefully"
  end

  # ============================================================================
  # Private: Simple Formatters (Production)
  # ============================================================================

  defp simple_task_started(ctx) do
    "worker=#{ctx.worker_name} flow=#{ctx.flow_slug} step=#{ctx.step_slug} " <>
      "run_id=#{ctx.run_id} task_index=#{ctx[:task_index] || 0} status=started"
  end

  defp simple_task_completed(ctx, duration_ms) do
    "worker=#{ctx.worker_name} flow=#{ctx.flow_slug} step=#{ctx.step_slug} " <>
      "run_id=#{ctx.run_id} task_index=#{ctx[:task_index] || 0} status=completed duration_ms=#{duration_ms}"
  end

  defp simple_task_failed(ctx, error, nil) do
    "worker=#{ctx.worker_name} flow=#{ctx.flow_slug} step=#{ctx.step_slug} " <>
      "run_id=#{ctx.run_id} task_index=#{ctx[:task_index] || 0} status=failed " <>
      "error=\"#{escape_quotes(error)}\""
  end

  defp simple_task_failed(ctx, error, retry_info) do
    base =
      "worker=#{ctx.worker_name} flow=#{ctx.flow_slug} step=#{ctx.step_slug} " <>
        "run_id=#{ctx.run_id} task_index=#{ctx[:task_index] || 0} status=failed " <>
        "error=\"#{escape_quotes(error)}\""

    if retry_info.attempt < retry_info.max_attempts do
      base <>
        " retry=#{retry_info.attempt + 1}/#{retry_info.max_attempts} retry_delay_s=#{retry_info.delay_seconds}"
    else
      base <> " retries_exhausted=true max_attempts=#{retry_info.max_attempts}"
    end
  end

  defp simple_startup_banner(ctx, flow) do
    "worker=#{ctx.worker_name} worker_id=#{ctx.worker_id} queue=#{ctx.queue_name} " <>
      "flow=#{flow.flow_slug} status=#{flow.status}"
  end

  # ============================================================================
  # Private: Helpers
  # ============================================================================

  defp log_format do
    Application.get_env(:pgflow, :log_format, default_format())
  end

  defp default_format do
    if Mix.env() == :prod do
      :simple
    else
      :fancy
    end
  rescue
    # Mix.env() not available at runtime in releases
    _ -> :simple
  end

  defp task_metadata(ctx) do
    [
      worker_name: ctx.worker_name,
      worker_id: ctx.worker_id,
      flow_slug: to_string(ctx.flow_slug),
      step_slug: to_string(ctx.step_slug),
      run_id: ctx.run_id,
      task_index: ctx[:task_index] || 0,
      msg_id: ctx[:msg_id]
    ]
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp retry_metadata(nil), do: []

  defp retry_metadata(retry_info) do
    [
      retry_attempt: retry_info.attempt,
      max_attempts: retry_info.max_attempts,
      retry_delay_s: retry_info.delay_seconds
    ]
  end

  defp short_id(id) when is_binary(id) do
    case String.split(id, "-") do
      [first | _] -> first
      _ -> String.slice(id, 0, 8)
    end
  end

  defp short_id(id), do: to_string(id)

  defp truncate_error(error) when byte_size(error) > 200 do
    String.slice(error, 0, 197) <> "..."
  end

  defp truncate_error(error), do: error

  defp pluralize_task(1), do: "task"
  defp pluralize_task(_), do: "tasks"

  defp escape_quotes(str) do
    String.replace(str, "\"", "\\\"")
  end
end
