defmodule PgFlowDashboard.Components.RunHistoryGrid do
  @moduledoc """
  GitHub-style activity grid showing run history by step.
  """

  use Phoenix.Component

  alias PgFlowDashboard.Components.RunHistoryHelpers

  @cell_size 16
  @cell_gap 3

  @doc """
  Renders a run history grid.

  ## Attributes

    * `:grid_data` - Map of step_slug => list of run results
    * `:steps` - Ordered list of step slugs for rows
    * `:max_runs` - Maximum number of runs to display
    * `:base_path` - Base path for navigation links

  """
  attr(:grid_data, :map, required: true)
  attr(:steps, :list, required: true)
  attr(:max_runs, :integer, default: 50)
  attr(:base_path, :string, default: "/pgflow")

  def run_history_grid(assigns) do
    # Get all runs (columns)
    all_runs =
      assigns.grid_data
      |> Map.values()
      |> List.flatten()
      |> Enum.uniq_by(& &1.run_id)
      |> Enum.sort_by(& &1.started_at, {:desc, DateTime})
      |> Enum.take(assigns.max_runs)
      |> Enum.reverse()

    assigns =
      assigns
      |> assign(:runs, all_runs)
      |> assign(:cell_size, @cell_size)
      |> assign(:cell_gap, @cell_gap)

    ~H"""
    <div class="overflow-x-auto">
      <div class="inline-block min-w-full">
        <!-- Header with run indicators -->
        <div class="flex items-end mb-1" style={"padding-left: 168px"}>
          <%= for _run <- @runs do %>
            <div
              class="flex-shrink-0"
              style={"width: #{@cell_size}px; margin-right: #{@cell_gap}px"}
            >
            </div>
          <% end %>
        </div>

        <!-- Grid rows -->
        <%= for step_slug <- @steps do %>
          <div class="flex items-center mb-1">
            <!-- Step label -->
            <div class="w-40 flex-shrink-0 pr-2 text-right">
              <span class="text-sm text-slate-600 dark:text-slate-300 truncate block">
                {format_step_label(step_slug)}
              </span>
            </div>

            <!-- Cells -->
            <div class="flex">
              <%= for run <- @runs do %>
                <.grid_cell
                  run={run}
                  step_slug={step_slug}
                  cell_data={get_cell_data(@grid_data, step_slug, run.run_id)}
                  cell_size={@cell_size}
                  cell_gap={@cell_gap}
                  base_path={@base_path}
                />
              <% end %>
            </div>
          </div>
        <% end %>

        <!-- Legend -->
        <div class="mt-4 flex items-center gap-4 text-sm text-slate-500 dark:text-slate-400">
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-emerald-500"></div>
            <span>Completed</span>
          </div>
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-rose-500"></div>
            <span>Failed</span>
          </div>
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-sky-500"></div>
            <span>Running</span>
          </div>
          <div class="flex items-center gap-1.5">
            <div class="w-4 h-4 rounded-sm bg-slate-300 dark:bg-slate-600"></div>
            <span>Pending</span>
          </div>
        </div>
      </div>
    </div>
    """
  end

  attr(:run, :map, required: true)
  attr(:step_slug, :string, required: true)
  attr(:cell_data, :map, default: nil)
  attr(:cell_size, :integer, required: true)
  attr(:cell_gap, :integer, required: true)
  attr(:base_path, :string, required: true)

  defp grid_cell(assigns) do
    status = if assigns.cell_data, do: assigns.cell_data.status, else: nil
    run_url = "#{assigns.base_path}/runs/#{assigns.run.run_id}?step=#{assigns.step_slug}"

    assigns =
      assigns
      |> assign(:status, status)
      |> assign(:cell_color, RunHistoryHelpers.cell_color(status))
      |> assign(:run_url, run_url)

    ~H"""
    <.link
      navigate={@run_url}
      class={[
        "flex-shrink-0 rounded-sm transition-all hover:scale-125 hover:z-10 block",
        "focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-1"
      ]}
      style={"width: #{@cell_size}px; height: #{@cell_size}px; margin-right: #{@cell_gap}px; background-color: #{@cell_color}"}
      aria-label={cell_label(@step_slug, @status, @run)}
      title={cell_tooltip(@cell_data, @run)}
    ></.link>
    """
  end

  defp get_cell_data(grid_data, step_slug, run_id) do
    grid_data
    |> Map.get(step_slug, [])
    |> Enum.find(fn cell -> cell.run_id == run_id end)
  end

  defp cell_label(step_slug, status, run) do
    status_text = if status, do: to_string(status), else: "pending"
    "#{format_step_label(step_slug)}: #{status_text} (run #{String.slice(run.run_id, 0..7)})"
  end

  defp cell_tooltip(nil, run) do
    "Run #{String.slice(run.run_id, 0..7)} - Not started"
  end

  defp cell_tooltip(cell_data, run) do
    status = cell_data.status || "pending"
    duration = RunHistoryHelpers.format_duration_ms(cell_data.duration_ms)
    "Run #{String.slice(run.run_id, 0..7)}\nStatus: #{status}\nDuration: #{duration}"
  end

  defp format_step_label(slug) when is_binary(slug) do
    label =
      slug
      |> String.split("_")
      |> Enum.map_join(" ", &String.capitalize/1)

    # Allow up to 24 chars before truncating
    if String.length(label) > 24 do
      String.slice(label, 0, 21) <> "..."
    else
      label
    end
  end

  defp format_step_label(slug) when is_atom(slug), do: slug |> to_string() |> format_step_label()
  defp format_step_label(_), do: "?"
end
