defmodule PgFlowDashboard.Components.RunHistoryHelpers do
  @moduledoc false

  use Phoenix.Component

  attr(:cells, :list, required: true)
  attr(:base_path, :string, required: true)

  @doc false
  def run_history_grid(assigns) do
    cells = Enum.reverse(assigns.cells)
    assigns = assign(assigns, :cells, cells)

    ~H"""
    <div class="overflow-x-auto">
      <%= if @cells == [] do %>
        <p class="text-sm text-slate-500 dark:text-slate-400">No run history yet</p>
      <% else %>
        <div class="flex flex-wrap gap-1">
          <%= for cell <- @cells do %>
            <.link
              navigate={"#{@base_path}/runs/#{cell.run_id}"}
              class="w-4 h-4 rounded-sm transition-all hover:scale-125 hover:z-10 block focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-1"
              style={"background-color: #{cell_color(cell.status)}"}
              title={cell_tooltip(cell)}
            ></.link>
          <% end %>
        </div>

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
        </div>
      <% end %>
    </div>
    """
  end

  @doc false
  def cell_color(status) do
    case status do
      s when s in [:completed, "completed"] -> "#10b981"
      s when s in [:failed, "failed"] -> "#ef4444"
      s when s in [:started, "started"] -> "#0ea5e9"
      _ -> "#cbd5e1"
    end
  end

  @doc false
  def format_duration_ms(nil), do: "-"
  def format_duration_ms(%Decimal{} = d), do: "#{round(Decimal.to_float(d))}ms"
  def format_duration_ms(ms) when is_float(ms), do: "#{round(ms)}ms"
  def format_duration_ms(ms) when is_integer(ms), do: "#{ms}ms"

  defp cell_tooltip(cell) do
    duration = format_duration_ms(cell.duration_ms)
    "Run #{String.slice(cell.run_id, 0..7)}\nStatus: #{cell.status}\nDuration: #{duration}"
  end
end
