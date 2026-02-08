defmodule PgFlowDashboard.Queries.Crons do
  @moduledoc """
  Database queries for cron-related data.

  Enriches cron data with calculated next run time using the Crontab library.
  """

  import PgFlow.Queries.Helpers

  alias Crontab.CronExpression.Parser, as: CronParser
  alias Crontab.Scheduler, as: CronScheduler

  @doc """
  Counts all crons.
  """
  @spec count_crons(module()) :: integer()
  def count_crons(repo) do
    execute_rpc(repo, "count_crons", [], schema: "pgflow_dashboard", mode: :count)
  end

  @doc """
  Lists crons with statistics and schedule info.

  Enriches each cron with:
  - `next_run_at` - The next scheduled run time (calculated from cron expression)
  - `human_schedule` - Human-readable schedule description

  ## Options
    * `:limit` - Maximum number of crons to return
    * `:cursor` - Cursor for pagination (flow_slug to start after)
  """
  @spec list_crons(module(), keyword()) :: list(map())
  def list_crons(repo, opts \\ []) do
    limit = Keyword.get(opts, :limit)
    cursor = Keyword.get(opts, :cursor)

    repo
    |> execute_rpc("list_crons", [limit, cursor], schema: "pgflow_dashboard", mode: :list)
    |> Enum.map(&enrich_cron/1)
  end

  @doc """
  Gets a cron's statistics and schedule info.

  Enriches the cron with:
  - `next_run_at` - The next scheduled run time (calculated from cron expression)
  - `human_schedule` - Human-readable schedule description
  """
  @spec get_cron(module(), String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def get_cron(repo, flow_slug) do
    case execute_rpc(repo, "get_cron", [flow_slug], schema: "pgflow_dashboard", mode: :single) do
      {:ok, cron} -> {:ok, enrich_cron(cron)}
      error -> error
    end
  end

  @doc """
  Gets run history data for a cron's activity grid.

  Returns a list of run result cells for the single perform step.
  """
  @spec get_run_history_grid(module(), String.t(), keyword()) :: list(map())
  def get_run_history_grid(repo, flow_slug, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)

    execute_rpc(repo, "get_run_history_grid", [flow_slug, limit],
      schema: "pgflow_dashboard",
      mode: :list
    )
  end

  # Enriches a cron record with calculated next run time and human-readable schedule
  defp enrich_cron(cron) do
    cron
    |> Map.put(:next_run_at, calculate_next_run(cron[:cron_expression]))
    |> Map.put(:human_schedule, humanize_schedule(cron[:cron_expression]))
  end

  # Calculates the next run time from a cron expression
  defp calculate_next_run(nil), do: nil

  defp calculate_next_run(expression) do
    with {:ok, cron_expr} <- CronParser.parse(expression),
         {:ok, next_run} <- CronScheduler.get_next_run_date(cron_expr, DateTime.utc_now()) do
      next_run
    else
      _ -> nil
    end
  end

  # Generates a human-readable description of the cron schedule
  defp humanize_schedule(nil), do: nil

  defp humanize_schedule(expression) do
    with {:ok, cron_expr} <- CronParser.parse(expression) do
      format_human_schedule(cron_expr)
    else
      _ -> nil
    end
  end

  # Format cron expression into human-readable text
  defp format_human_schedule(%Crontab.CronExpression{} = expr) do
    parts = []

    # Time part
    time_part = format_time(expr.minute, expr.hour)
    parts = if time_part, do: [time_part | parts], else: parts

    # Day part
    day_part = format_days(expr.day, expr.month, expr.weekday)
    parts = if day_part, do: [day_part | parts], else: parts

    case parts do
      [] -> "Custom schedule"
      _ -> parts |> Enum.reverse() |> Enum.join(" ")
    end
  end

  defp format_time([:*], [:*]), do: "Every minute"

  defp format_time([{:/, :*, step}], [:*]) when is_integer(step) do
    "Every #{step} minutes"
  end

  defp format_time(minutes, [:*]) when is_list(minutes) do
    case minutes do
      [0] -> "Hourly"
      [min] when is_integer(min) -> "At minute #{min} every hour"
      _ -> nil
    end
  end

  defp format_time([0], [hour]) when is_integer(hour), do: "Daily at #{format_hour(hour)}"

  defp format_time([min], [hour]) when is_integer(min) and is_integer(hour) do
    "Daily at #{format_hour(hour)}:#{String.pad_leading(to_string(min), 2, "0")}"
  end

  defp format_time(minutes, [{:-, start_hour, end_hour}]) when is_list(minutes) do
    case minutes do
      [{:/, :*, step}] ->
        "Every #{step} min from #{format_hour(start_hour)}-#{format_hour(end_hour)}"

      _ ->
        nil
    end
  end

  defp format_time(_minutes, _hours), do: nil

  defp format_days([:*], [:*], [:*]), do: nil

  @weekday_names %{
    0 => "Sundays",
    1 => "Mondays",
    2 => "Tuesdays",
    3 => "Wednesdays",
    4 => "Thursdays",
    5 => "Fridays",
    6 => "Saturdays"
  }

  defp format_days([:*], [:*], [{:-, 1, 5}]), do: "Weekdays"
  defp format_days([:*], [:*], [{:-, 0, 6}]), do: nil

  defp format_days([:*], [:*], [day]) when is_map_key(@weekday_names, day) do
    @weekday_names[day]
  end

  defp format_days([:*], [:*], _weekdays), do: nil

  defp format_days([1], [:*], [:*]), do: "Monthly"
  defp format_days([day], [:*], [:*]) when is_integer(day), do: "On day #{day} monthly"
  defp format_days(_days, _months, _weekdays), do: nil

  defp format_hour(hour), do: "#{hour}:00"
end
