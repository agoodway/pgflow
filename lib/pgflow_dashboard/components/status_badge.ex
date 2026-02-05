defmodule PgFlowDashboard.Components.StatusBadge do
  @moduledoc """
  Status badge component with WCAG AA compliant colors.
  """

  use Phoenix.Component

  @doc """
  Renders a status badge.

  ## Attributes

    * `:status` - The status to display (completed, failed, started, created)
    * `:size` - Badge size (:sm, :md, :lg). Default: :md
    * `:pulse` - Whether to show pulse animation for active states. Default: false

  """
  attr(:status, :any, required: true)
  attr(:size, :atom, default: :md)
  attr(:pulse, :boolean, default: false)

  def status_badge(assigns) do
    normalized = normalize_status(assigns.status)

    assigns =
      assigns
      |> assign(:status_text, status_text(normalized))
      |> assign(:status_classes, status_classes(normalized))
      |> assign(:size_classes, size_classes(assigns.size))
      |> assign(:is_active, normalized == :started)

    ~H"""
    <span class={[
      "inline-flex items-center gap-1.5 font-medium rounded-full",
      @status_classes,
      @size_classes
    ]}>
      <span :if={@pulse && @is_active} class="relative flex h-2 w-2">
        <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-current opacity-75"></span>
        <span class="relative inline-flex rounded-full h-2 w-2 bg-current"></span>
      </span>
      <span :if={!@pulse || !@is_active} class="h-1.5 w-1.5 rounded-full bg-current"></span>
      {@status_text}
    </span>
    """
  end

  defp normalize_status(status) when is_atom(status), do: status
  defp normalize_status("completed"), do: :completed
  defp normalize_status("failed"), do: :failed
  defp normalize_status("started"), do: :started
  defp normalize_status("created"), do: :created
  defp normalize_status(status) when is_binary(status), do: String.to_existing_atom(status)
  defp normalize_status(_), do: :unknown

  defp status_text(:completed), do: "Completed"
  defp status_text(:failed), do: "Failed"
  defp status_text(:started), do: "Running"
  defp status_text(:created), do: "Pending"
  defp status_text(status) when is_atom(status), do: status |> to_string() |> String.capitalize()
  defp status_text(_), do: "Unknown"

  # WCAG AA compliant colors
  defp status_classes(:completed) do
    "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400"
  end

  defp status_classes(:failed) do
    "bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-400"
  end

  defp status_classes(:started) do
    "bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-400"
  end

  defp status_classes(_) do
    "bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-400"
  end

  defp size_classes(:sm), do: "px-2 py-0.5 text-xs"
  defp size_classes(:md), do: "px-2.5 py-1 text-xs"
  defp size_classes(:lg), do: "px-3 py-1.5 text-sm"
end
