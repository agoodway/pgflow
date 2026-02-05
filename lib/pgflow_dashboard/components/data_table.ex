defmodule PgFlowDashboard.Components.DataTable do
  @moduledoc """
  Sortable, filterable data table component.
  """

  use Phoenix.Component

  @doc """
  Renders a data table.

  ## Attributes

    * `:rows` - List of row data (maps)
    * `:columns` - List of column definitions

  ## Column Definition

  Each column is a map with:
    * `:key` - The key to extract from each row
    * `:label` - Display label
    * `:class` - Optional CSS class for the column

  """
  attr(:rows, :list, required: true)
  attr(:columns, :list, required: true)
  attr(:row_click, :any, default: nil)
  attr(:empty_message, :string, default: "No data available")

  def data_table(assigns) do
    ~H"""
    <div class="overflow-x-auto">
      <table class="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
        <thead class="bg-slate-50 dark:bg-slate-800/50">
          <tr>
            <%= for col <- @columns do %>
              <th
                scope="col"
                class={[
                  "px-4 py-3 text-left text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider",
                  col[:class]
                ]}
              >
                {col.label}
              </th>
            <% end %>
          </tr>
        </thead>
        <tbody class="bg-white dark:bg-slate-800 divide-y divide-slate-200 dark:divide-slate-700">
          <%= if @rows == [] do %>
            <tr>
              <td colspan={length(@columns)} class="px-4 py-8 text-center text-slate-500 dark:text-slate-400">
                {@empty_message}
              </td>
            </tr>
          <% else %>
            <%= for row <- @rows do %>
              <tr class={[
                "hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors",
                @row_click && "cursor-pointer"
              ]}>
                <%= for col <- @columns do %>
                  <td class={[
                    "px-4 py-3 text-sm text-slate-700 dark:text-slate-300",
                    col[:class]
                  ]}>
                    {render_cell(Map.get(row, col.key), col)}
                  </td>
                <% end %>
              </tr>
            <% end %>
          <% end %>
        </tbody>
      </table>
    </div>
    """
  end

  defp render_cell(value, %{render: render_fn}) when is_function(render_fn, 1) do
    render_fn.(value)
  end

  defp render_cell(nil, _col), do: "-"
  defp render_cell(value, _col), do: to_string(value)
end
