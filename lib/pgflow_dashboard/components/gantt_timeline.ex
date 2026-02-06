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
    run_start = assigns.run.started_at
    run_end = assigns.run.completed_at || DateTime.utc_now()

    total_duration_ms = DateTime.diff(run_end, run_start, :millisecond)
    # Ensure minimum duration to avoid division by zero
    total_duration_ms = max(total_duration_ms, 1000)

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

      <div class="p-4 overflow-x-auto">
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
              <%= if step.started_at do %>
                <%
                  bar_start = calc_position(step.started_at, @run_start, @total_duration_ms, @chart_width)
                  bar_end = calc_position(step.completed_at || DateTime.utc_now(), @run_start, @total_duration_ms, @chart_width)
                  bar_width = max(bar_end - bar_start, 4)
                %>
                <rect
                  x={@label_width + @padding + bar_start}
                  y="6"
                  width={bar_width}
                  height={@row_height - 12}
                  rx="3"
                  class={bar_color(step.status)}
                />

                <!-- Duration label - centered in bar if wide, or to the right if narrow -->
                <%= if bar_width > 45 do %>
                  <text
                    x={@label_width + @padding + bar_start + bar_width / 2}
                    y={@row_height / 2 + 4}
                    class="fill-white text-xs font-medium"
                    text-anchor="middle"
                  >
                    {format_duration(step.duration_ms || 0)}
                  </text>
                <% else %>
                  <text
                    x={@label_width + @padding + bar_start + bar_width + 4}
                    y={@row_height / 2 + 4}
                    class="fill-current text-xs"
                    text-anchor="start"
                  >
                    {format_duration(step.duration_ms || 0)}
                  </text>
                <% end %>
              <% else %>
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
            <% now_pos = calc_position(DateTime.utc_now(), @run_start, @total_duration_ms, @chart_width) %>
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
      <div class="px-4 py-2 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 flex items-center gap-4 text-xs">
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
          <span class="w-3 h-3 rounded bg-slate-300 dark:bg-slate-600"></span>
          <span class="text-slate-600 dark:text-slate-400">Pending</span>
        </div>
      </div>
    </div>
    """
  end

  defp calc_position(datetime, run_start, total_duration_ms, chart_width) do
    offset_ms = DateTime.diff(datetime, run_start, :millisecond)
    ratio = offset_ms / total_duration_ms
    Float.round(ratio * chart_width, 1)
  end

  defp bar_color("completed"), do: "fill-emerald-500"
  defp bar_color("started"), do: "fill-blue-500"
  defp bar_color("failed"), do: "fill-red-500"
  defp bar_color(_), do: "fill-slate-300 dark:fill-slate-600"

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
