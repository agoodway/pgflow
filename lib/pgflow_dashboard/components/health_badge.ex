defmodule PgFlowDashboard.Components.HealthBadge do
  @moduledoc """
  Health status badge component for workers.
  """

  use Phoenix.Component

  @doc """
  Renders a health status badge.

  ## Attributes

    * `:status` - The health status ("healthy", "stale", "dead")
    * `:size` - Badge size (:sm, :md). Default: :md

  """
  attr :status, :string, required: true
  attr :size, :atom, default: :md, values: [:sm, :md]

  def health_badge(assigns) do
    {color, text} =
      case assigns.status do
        "healthy" ->
          {"bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400",
           "Healthy"}

        "stale" ->
          {"bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400", "Stale"}

        _ ->
          {"bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400", "Dead"}
      end

    assigns =
      assigns
      |> assign(:color, color)
      |> assign(:text, text)
      |> assign(:size_classes, size_classes(assigns.size))
      |> assign(:dot_classes, dot_classes(assigns.size))

    ~H"""
    <span class={["inline-flex items-center gap-1.5 font-medium rounded-full", @color, @size_classes]}>
      <span class={["rounded-full bg-current", @dot_classes]}></span>
      {@text}
    </span>
    """
  end

  defp size_classes(:sm), do: "px-2 py-0.5 text-xs"
  defp size_classes(:md), do: "px-2.5 py-1 text-sm"

  defp dot_classes(:sm), do: "w-1.5 h-1.5"
  defp dot_classes(:md), do: "w-2 h-2"
end
