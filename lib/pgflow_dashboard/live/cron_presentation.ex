defmodule PgFlowDashboard.Live.CronPresentation do
  @moduledoc false

  alias Crontab.CronExpression.Parser, as: CronParser
  alias PgFlow.CronSummary

  @weekday_names %{
    0 => "Sundays",
    1 => "Mondays",
    2 => "Tuesdays",
    3 => "Wednesdays",
    4 => "Thursdays",
    5 => "Fridays",
    6 => "Saturdays"
  }

  @enforce_keys [:cron, :human_schedule]
  defstruct [:cron, :human_schedule]

  @type t :: %__MODULE__{cron: CronSummary.t(), human_schedule: String.t() | nil}

  @doc """
  Builds a dashboard presentation with a human-readable cron schedule.
  """
  @spec present(CronSummary.t()) :: t()
  def present(%CronSummary{cron_expression: expression} = cron) do
    %__MODULE__{cron: cron, human_schedule: humanize_schedule(expression)}
  end

  defp humanize_schedule(nil), do: nil

  defp humanize_schedule(expression) do
    case CronParser.parse(expression) do
      {:ok, cron_expression} -> format_human_schedule(cron_expression)
      _ -> nil
    end
  end

  defp format_human_schedule(%Crontab.CronExpression{} = expression) do
    [
      format_time(expression.minute, expression.hour),
      format_days(expression.day, expression.month, expression.weekday)
    ]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> "Custom schedule"
      parts -> Enum.join(parts, " ")
    end
  end

  defp format_time([:*], [:*]), do: "Every minute"

  defp format_time([{:/, :*, step}], [:*]) when is_integer(step),
    do: "Every #{step} minutes"

  defp format_time([0], [:*]), do: "Hourly"

  defp format_time([minute], [:*]) when is_integer(minute),
    do: "At minute #{minute} every hour"

  defp format_time([0], [hour]) when is_integer(hour), do: "Daily at #{format_hour(hour)}"

  defp format_time([minute], [hour]) when is_integer(minute) and is_integer(hour) do
    "Daily at #{format_hour(hour)}:#{String.pad_leading(to_string(minute), 2, "0")}"
  end

  defp format_time([{:/, :*, step}], [{:-, start_hour, end_hour}]) do
    "Every #{step} min from #{format_hour(start_hour)}-#{format_hour(end_hour)}"
  end

  defp format_time(_minutes, _hours), do: nil

  defp format_days([:*], [:*], [:*]), do: nil
  defp format_days([:*], [:*], [{:-, 1, 5}]), do: "Weekdays"
  defp format_days([:*], [:*], [{:-, 0, 6}]), do: nil

  defp format_days([:*], [:*], [day]) when is_map_key(@weekday_names, day),
    do: @weekday_names[day]

  defp format_days([:*], [:*], _weekdays), do: nil
  defp format_days([1], [:*], [:*]), do: "Monthly"

  defp format_days([day], [:*], [:*]) when is_integer(day),
    do: "On day #{day} monthly"

  defp format_days(_days, _months, _weekdays), do: nil

  defp format_hour(hour), do: "#{hour}:00"
end
