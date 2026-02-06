defmodule PgFlowDashboard.Components.TypeBadge do
  @moduledoc """
  Type badge component for distinguishing between flows and jobs.

  Flows are considered the "default" type, so the badge is only shown for jobs.
  """

  use Phoenix.Component

  @doc """
  Renders a type badge for jobs.

  Jobs get a small "job" pill badge. Flows are the default type and don't
  need a badge (the absence of a badge implies "flow").

  ## Attributes

    * `:type` - The flow type ("flow" or "job"). Only "job" renders a badge.

  ## Examples

      <TypeBadge.type_badge type="job" />
      <TypeBadge.type_badge type={@flow_type} />

  """
  attr(:type, :string, default: "flow")

  def type_badge(assigns) do
    ~H"""
    <span
      :if={@type == "job"}
      class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400"
    >
      job
    </span>
    """
  end
end
