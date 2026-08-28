defmodule PgFlowDashboard.Components.GanttTimeline do
  @moduledoc """
  Gantt timeline component showing step execution timing within a run.
  Renders as SVG for crisp visuals and easy theming.
  """

  use Phoenix.Component

  @doc """
  Renders a Gantt timeline for a run's steps.

  ## Assigns
    * `:run` - The run map with started_at, completed_at, status
    * `:step_states` - List of step state maps with step_slug, started_at, completed_at, status
  """
  attr(:run, :map, required: true)
  attr(:step_states, :list, required: true)

  def gantt_timeline(assigns) do
    # Calculate timeline bounds
    current_time = DateTime.utc_now()
    run_start = assigns.run.started_at
    run_end = assigns.run.completed_at || current_time

    total_duration_ms = max(DateTime.diff(run_end, run_start, :millisecond), 0)
    timeline_duration_ms = max(total_duration_ms, 1)

    # Sort steps by start time (nil starts go last)
    sorted_steps =
      Enum.sort_by(assigns.step_states, fn step ->
        case step.started_at do
          nil -> {1, step.step_slug}
          dt -> {0, DateTime.to_unix(dt)}
        end
      end)

    # Dimensions
    row_height = 32
    label_width = 180
    chart_width = 460
    padding = 8
    header_height = 24
    total_height = header_height + length(sorted_steps) * row_height + padding

    assigns =
      assigns
      |> assign(:sorted_steps, sorted_steps)
      |> assign(:run_start, run_start)
      |> assign(:run_end, run_end)
      |> assign(:total_duration_ms, total_duration_ms)
      |> assign(:timeline_duration_ms, timeline_duration_ms)
      |> assign(:row_height, row_height)
      |> assign(:label_width, label_width)
      |> assign(:chart_width, chart_width)
      |> assign(:padding, padding)
      |> assign(:header_height, header_height)
      |> assign(:total_height, total_height)

    ~H"""
    <div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
      <div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700">
        <h3 class="text-sm font-semibold text-slate-900 dark:text-white">Timeline</h3>
        <p class="text-xs text-slate-500 dark:text-slate-400 mt-1">
          Step execution timing · Total: {format_duration(@total_duration_ms)}
        </p>
      </div>

      <div class="p-4 overflow-x-auto" role="region" tabindex="0" aria-label="Timeline chart">
        <svg
          width={@label_width + @chart_width + @padding * 2}
          height={@total_height}
          class="text-slate-600 dark:text-slate-400"
        >
          <!-- Time axis header -->
          <g transform={"translate(#{@label_width + @padding}, 0)"}>
            <!-- Start time -->
            <text x="0" y="16" class="fill-current text-xs" text-anchor="start">
              0s
            </text>
            <!-- End time -->
            <text x={@chart_width} y="16" class="fill-current text-xs" text-anchor="end">
              {format_duration(@total_duration_ms)}
            </text>
            <!-- Middle marker -->
            <text x={@chart_width / 2} y="16" class="fill-current text-xs" text-anchor="middle">
              {format_duration(div(@total_duration_ms, 2))}
            </text>
          </g>

          <!-- Step rows -->
          <%= for {step, idx} <- Enum.with_index(@sorted_steps) do %>
            <g transform={"translate(0, #{@header_height + idx * @row_height})"}>
              <!-- Step label -->
              <text
                x={@label_width - 8}
                y={@row_height / 2 + 4}
                class="fill-current text-xs"
                text-anchor="end"
              >
                {truncate_label(step.step_slug, 24)}
              </text>

              <!-- Row background (alternating) -->
              <rect
                x={@label_width + @padding}
                y="2"
                width={@chart_width}
                height={@row_height - 4}
                rx="2"
                class={if rem(idx, 2) == 0, do: "fill-slate-50 dark:fill-slate-700/30", else: "fill-transparent"}
              />

              <!-- Step bar -->
              <%= cond do %>
                <% step.started_at -> %>
                  <%
                    bar_end_at =
                      if step.status == "skipped" do
                        step.skipped_at || step.started_at
                      else
                        step.completed_at || @run_end
                      end

                    bar_start = calc_position(step.started_at, @run_start, @timeline_duration_ms, @chart_width)
                    bar_end = calc_position(bar_end_at, @run_start, @timeline_duration_ms, @chart_width)
                    bar_width = max(bar_end - bar_start, 4)
                  %>
                  <.step_bar
                    x={@label_width + @padding + bar_start}
                    width={bar_width}
                    row_height={@row_height}
                    status={step.status}
                    duration_ms={step_duration_ms(step)}
                  />

                <% step.status == "skipped" -> %>
                  <!-- Never-started skipped step: ghost/zero-width marker, not the
                       dashed "pending" bar - it never had a chance to run. -->
                  <circle
                    class="gantt-skip-ghost fill-orange-600 stroke-orange-900 dark:fill-amber-400 dark:stroke-amber-200"
                    cx={@label_width + @padding + 2}
                    cy={@row_height / 2 - 2}
                    r="3"
                    stroke-width="1"
                  />
                  <text
                    x={@label_width + @padding + 10}
                    y={@row_height / 2 + 2}
                    class="fill-current text-xs"
                    text-anchor="start"
                  >
                    Skipped
                  </text>

                <% true -> %>
                  <!-- Pending indicator -->
                  <rect
                    x={@label_width + @padding}
                    y={@row_height / 2 - 2}
                    width={@chart_width}
                    height="4"
                    rx="2"
                    class="fill-slate-200 dark:fill-slate-600"
                    stroke-dasharray="4,4"
                  />
              <% end %>
            </g>
          <% end %>

          <!-- Vertical grid lines -->
          <g transform={"translate(#{@label_width + @padding}, #{@header_height})"} class="stroke-slate-200 dark:stroke-slate-700">
            <line x1="0" y1="0" x2="0" y2={length(@sorted_steps) * @row_height} stroke-width="1" />
            <line x1={@chart_width / 4} y1="0" x2={@chart_width / 4} y2={length(@sorted_steps) * @row_height} stroke-width="1" stroke-dasharray="2,2" />
            <line x1={@chart_width / 2} y1="0" x2={@chart_width / 2} y2={length(@sorted_steps) * @row_height} stroke-width="1" stroke-dasharray="2,2" />
            <line x1={@chart_width * 3 / 4} y1="0" x2={@chart_width * 3 / 4} y2={length(@sorted_steps) * @row_height} stroke-width="1" stroke-dasharray="2,2" />
            <line x1={@chart_width} y1="0" x2={@chart_width} y2={length(@sorted_steps) * @row_height} stroke-width="1" />
          </g>

          <!-- "Now" indicator for running runs -->
          <%= if @run.status == "started" do %>
            <% now_pos = calc_position(@run_end, @run_start, @timeline_duration_ms, @chart_width) %>
            <g transform={"translate(#{@label_width + @padding + now_pos}, #{@header_height})"}>
              <line
                x1="0" y1="0"
                x2="0" y2={length(@sorted_steps) * @row_height}
                class="stroke-purple-500"
                stroke-width="2"
              />
              <polygon
                points="-4,0 4,0 0,6"
                class="fill-purple-500"
              />
            </g>
          <% end %>
        </svg>
      </div>

      <!-- Legend -->
      <div class="px-4 py-2 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex flex-wrap items-center gap-x-4 gap-y-2 text-xs">
        <div class="flex items-center gap-1.5">
          <span class="w-3 h-3 rounded bg-emerald-500"></span>
          <span class="text-slate-600 dark:text-slate-400">Completed</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="w-3 h-3 rounded bg-blue-500"></span>
          <span class="text-slate-600 dark:text-slate-400">Running</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="w-3 h-3 rounded bg-red-500"></span>
          <span class="text-slate-600 dark:text-slate-400">Failed</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="w-3 h-3 rounded bg-orange-600 ring-1 ring-orange-800/40 dark:bg-amber-400 dark:ring-amber-200/50"></span>
          <span class="text-slate-600 dark:text-slate-400">Skipped</span>
        </div>
        <div class="flex items-center gap-1.5">
          <span class="w-3 h-3 rounded bg-slate-300 dark:bg-slate-600"></span>
          <span class="text-slate-600 dark:text-slate-400">Pending</span>
        </div>
      </div>
    </div>
    """
  end

  @doc false
  # Shared bar markup for any step that has a start time (completed, running,
  # failed, or skipped-after-starting). States differ only in bar color
  # (via `bar_color/1`) and the timestamp used to compute `width` upstream -
  # the rect/label structure itself is identical across states.
  attr(:x, :float, required: true)
  attr(:width, :float, required: true)
  attr(:row_height, :integer, required: true)
  attr(:status, :string, required: true)
  attr(:duration_ms, :any, required: true)

  defp step_bar(assigns) do
    ~H"""
    <rect
      x={@x}
      y="6"
      width={@width}
      height={@row_height - 12}
      rx="3"
      class={bar_color(@status)}
    />

    <!-- Duration label - centered in bar if wide, or to the right if narrow -->
    <%= if @width > 45 do %>
      <text
        x={@x + @width / 2}
        y={@row_height / 2 + 4}
        class={[bar_text_color(@status), "text-xs font-medium"]}
        text-anchor="middle"
      >
        {format_duration(@duration_ms || 0)}
      </text>
    <% else %>
      <text
        x={@x + @width + 4}
        y={@row_height / 2 + 4}
        class="fill-current text-xs"
        text-anchor="start"
      >
        {format_duration(@duration_ms || 0)}
      </text>
    <% end %>
    """
  end

  defp step_duration_ms(step) do
    Map.get_lazy(step, :duration_ms, fn ->
      finished_at = step.completed_at || step.skipped_at || step.failed_at

      if step.started_at && finished_at do
        max(DateTime.diff(finished_at, step.started_at, :millisecond), 0)
      end
    end)
  end

  defp calc_position(datetime, run_start, total_duration_ms, chart_width) do
    offset_ms = DateTime.diff(datetime, run_start, :millisecond)
    ratio = offset_ms / total_duration_ms

    ratio
    |> Kernel.*(chart_width)
    |> max(0.0)
    |> min(chart_width * 1.0)
    |> Float.round(1)
  end

  defp bar_color("completed"), do: "fill-emerald-500"
  defp bar_color("started"), do: "fill-blue-500"
  defp bar_color("failed"), do: "fill-red-500"
  defp bar_color("skipped"), do: "fill-orange-600 dark:fill-amber-400"
  defp bar_color(_), do: "fill-slate-300 dark:fill-slate-600"

  defp bar_text_color("skipped"), do: "fill-white dark:fill-slate-950"
  defp bar_text_color(_), do: "fill-white"

  defp format_duration(ms) when is_number(ms) do
    format_duration_value(ms)
  end

  defp format_duration(%Decimal{} = ms) do
    format_duration_value(Decimal.to_float(ms))
  end

  defp format_duration(ms) when is_binary(ms) do
    case Float.parse(ms) do
      {num, _} -> format_duration_value(num)
      :error -> "-"
    end
  end

  defp format_duration(_), do: "-"

  defp format_duration_value(ms) do
    cond do
      ms < 1000 -> "#{round(ms)}ms"
      ms < 60_000 -> "#{Float.round(ms / 1000, 1)}s"
      ms < 3_600_000 -> "#{Float.round(ms / 60_000, 1)}m"
      true -> "#{Float.round(ms / 3_600_000, 1)}h"
    end
  end

  defp truncate_label(label, max_len) do
    if String.length(label) > max_len do
      String.slice(label, 0, max_len - 1) <> "…"
    else
      label
    end
  end
end
