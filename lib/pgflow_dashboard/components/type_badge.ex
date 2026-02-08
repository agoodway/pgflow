defmodule PgFlowDashboard.Components.TypeBadge do
  @moduledoc """
  Type badge component for distinguishing between flows, jobs, and crons.
  """

  use Phoenix.Component

  @doc """
  Renders a type badge for flows, jobs, and crons.

  Flows get a small purple "flow" pill badge.
  Jobs get a small blue "job" pill badge.
  Crons get a small amber "cron" pill badge.

  ## Attributes

    * `:type` - The flow type ("flow", "job", or "cron").

  ## Examples

      <TypeBadge.type_badge type="flow" />
      <TypeBadge.type_badge type="job" />
      <TypeBadge.type_badge type="cron" />
      <TypeBadge.type_badge type={@flow_type} />

  """
  attr(:type, :string, default: "flow")

  def type_badge(assigns) do
    ~H"""
    <span
      :if={@type == "flow"}
      class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400"
    >
      flow
    </span>
    <span
      :if={@type == "job"}
      class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400"
    >
      job
    </span>
    <span
      :if={@type == "cron"}
      class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400"
    >
      cron
    </span>
    """
  end
end
