defmodule PgFlowDashboard.Components.ProgressBar do
  @moduledoc """
  Progress bar component for run progress visualization.
  """

  use Phoenix.Component

  @doc """
  Renders a progress bar.

  ## Attributes

    * `:progress` - Progress percentage (0-100)
    * `:completed` - Number of completed steps
    * `:total` - Total number of steps
    * `:failed` - Number of failed steps (optional)
    * `:skipped` - Number of skipped steps (optional). Skipped is a terminal
      state, so it renders as its own segment alongside completed/failed
      rather than being folded into either.
    * `:size` - Bar size (:sm, :md, :lg). Default: :md

  """
  attr(:progress, :any, default: 0)
  attr(:completed, :integer, default: 0)
  attr(:total, :integer, default: 0)
  attr(:failed, :integer, default: 0)
  attr(:skipped, :integer, default: 0)
  attr(:size, :atom, default: :md)

  def progress_bar(assigns) do
    progress = parse_progress(assigns.progress)
    assigns = assign(assigns, :progress_pct, progress)
    assigns = assign(assigns, :height_class, height_class(assigns.size))

    ~H"""
    <div class="w-full">
      <div class={["w-full bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden", @height_class]}>
        <div class="h-full flex">
          <div
            class="h-full bg-emerald-500 dark:bg-emerald-400 transition-all duration-300"
            style={"width: #{success_width(@completed, @failed, @total)}%"}
          />
          <div
            :if={@skipped > 0}
            class="h-full bg-amber-400 dark:bg-amber-500"
            style={"width: #{skipped_width(@skipped, @total)}%"}
          />
          <div
            :if={@failed > 0}
            class="h-full bg-rose-500 dark:bg-rose-400"
            style={"width: #{failed_width(@failed, @total)}%"}
          />
        </div>
      </div>
      <div :if={@total > 0} class="mt-1 flex items-center justify-between text-xs text-slate-500 dark:text-slate-400">
        <span>{@completed}/{@total} steps<span :if={@skipped > 0}> &middot; {@skipped} skipped</span></span>
        <span>{@progress_pct}%</span>
      </div>
    </div>
    """
  end

  defp parse_progress(progress) when is_number(progress) do
    progress
    |> Decimal.new()
    |> Decimal.round(1)
    |> Decimal.to_string()
  rescue
    _ -> "0"
  end

  defp parse_progress(%Decimal{} = progress) do
    progress
    |> Decimal.round(1)
    |> Decimal.to_string()
  end

  defp parse_progress(_), do: "0"

  defp success_width(completed, failed, total) when total > 0 do
    ((completed - failed) / total * 100)
    |> max(0.0)
    |> Float.round(1)
  end

  defp success_width(_, _, _), do: 0.0

  defp failed_width(failed, total) when total > 0 do
    (failed / total * 100)
    |> Float.round(1)
  end

  defp failed_width(_, _), do: 0.0

  defp skipped_width(skipped, total) when total > 0 do
    (skipped / total * 100)
    |> Float.round(1)
  end

  defp skipped_width(_, _), do: 0.0

  defp height_class(:sm), do: "h-1"
  defp height_class(:md), do: "h-2"
  defp height_class(:lg), do: "h-3"
end
