defmodule PgFlowDashboard.Components.MetricCard do
  @moduledoc """
  Dashboard metric card component.
  """

  use Phoenix.Component

  @doc """
  Renders a metric card.

  ## Attributes

    * `:title` - The metric title
    * `:value` - The metric value
    * `:subtitle` - Optional subtitle text
    * `:trend` - Optional trend indicator (:up, :down, :neutral)
    * `:trend_value` - Optional trend percentage/value

  """
  attr(:title, :string, required: true)
  attr(:value, :any, required: true)
  attr(:subtitle, :string, default: nil)
  attr(:trend, :atom, default: nil)
  attr(:trend_value, :string, default: nil)
  attr(:class, :string, default: nil)
  attr(:href, :string, default: nil)

  def metric_card(assigns) do
    ~H"""
    <.card_wrapper href={@href} class={@class}>
      <div class="flex items-start justify-between">
        <div>
          <p class="text-sm font-medium text-slate-500 dark:text-slate-400">{@title}</p>
          <p class="mt-1 text-2xl font-semibold text-slate-900 dark:text-white">{format_value(@value)}</p>
          <p :if={@subtitle} class="mt-1 text-xs text-slate-500 dark:text-slate-400">{@subtitle}</p>
        </div>

        <div :if={@trend && @trend_value} class={[
          "flex items-center gap-0.5 text-sm font-medium",
          @trend == :up && "text-emerald-600 dark:text-emerald-400",
          @trend == :down && "text-rose-600 dark:text-rose-400",
          @trend == :neutral && "text-slate-500 dark:text-slate-400"
        ]}>
          <.trend_icon trend={@trend} />
          {@trend_value}
        </div>
      </div>
    </.card_wrapper>
    """
  end

  attr(:href, :string, default: nil)
  attr(:class, :string, default: nil)
  slot(:inner_block, required: true)

  defp card_wrapper(%{href: nil} = assigns) do
    ~H"""
    <div class={[
      "bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4",
      @class
    ]}>
      {render_slot(@inner_block)}
    </div>
    """
  end

  defp card_wrapper(assigns) do
    ~H"""
    <.link
      navigate={@href}
      class={[
        "block bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 transition-colors cursor-pointer hover:border-purple-300 dark:hover:border-purple-600",
        @class
      ]}
    >
      {render_slot(@inner_block)}
    </.link>
    """
  end

  defp trend_icon(%{trend: :up} = assigns) do
    ~H"""
    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 10l7-7m0 0l7 7m-7-7v18" />
    </svg>
    """
  end

  defp trend_icon(%{trend: :down} = assigns) do
    ~H"""
    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 14l-7 7m0 0l-7-7m7 7V3" />
    </svg>
    """
  end

  defp trend_icon(assigns) do
    ~H"""
    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14" />
    </svg>
    """
  end

  defp format_value(value) when is_integer(value),
    do: Number.Delimit.number_to_delimited(value, precision: 0)

  defp format_value(value) when is_float(value),
    do: Number.Delimit.number_to_delimited(value, precision: 1)

  defp format_value(value), do: value
end

# Simple number formatting module
defmodule Number.Delimit do
  @moduledoc false

  def number_to_delimited(number, opts \\ []) do
    precision = Keyword.get(opts, :precision, 2)
    delimiter = Keyword.get(opts, :delimiter, ",")

    number
    |> format_number(precision)
    |> add_delimiter(delimiter)
  end

  defp format_number(number, 0) when is_integer(number), do: Integer.to_string(number)

  defp format_number(number, 0) when is_float(number),
    do: number |> round() |> Integer.to_string()

  defp format_number(number, precision) when is_float(number),
    do: :erlang.float_to_binary(number, decimals: precision)

  defp format_number(number, _precision), do: to_string(number)

  defp add_delimiter(str, delimiter) do
    case String.split(str, ".") do
      [int_part] -> delimit_integer(int_part, delimiter)
      [int_part, dec_part] -> delimit_integer(int_part, delimiter) <> "." <> dec_part
    end
  end

  defp delimit_integer(str, delimiter) do
    str
    |> String.graphemes()
    |> Enum.reverse()
    |> Enum.chunk_every(3)
    |> Enum.join(delimiter)
    |> String.reverse()
  end
end
