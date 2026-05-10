defmodule PgflowDemoWeb.Components.PoweredBy do
  @moduledoc """
  Reusable "Powered by" component showing the tech stack.
  Supports different sizes for header vs footer usage.
  """

  use Phoenix.Component

  @doc """
  Renders the "Powered by" tech stack with links.

  ## Sizes
  - `:sm` - Small, muted text for footers (default)
  - `:md` - Medium, slightly brighter for headers

  ## Examples

      <PoweredBy.powered_by size={:sm} />
      <PoweredBy.powered_by size={:md} />
  """
  attr :size, :atom, default: :sm, values: [:sm, :md]
  attr :class, :string, default: ""

  def powered_by(assigns) do
    ~H"""
    <p class={[container_class(@size), @class]}>
      Powered by <.tech_link href="https://www.postgresql.org" size={@size}>PostgreSQL</.tech_link>(<.tech_link
        href="https://github.com/pgmq/pgmq"
        size={@size}
      >
        PGMQ
      </.tech_link>, <.tech_link href="https://github.com/citusdata/pg_cron" size={@size}>pg_cron</.tech_link>, <.tech_link
        href="https://pgflow.dev"
        size={@size}
      >PgFlow</.tech_link>),
      <.tech_link href="https://elixir-lang.org" size={@size}>Elixir</.tech_link>
      and <.tech_link href="https://phoenixframework.org" size={@size}>Phoenix LiveView</.tech_link>
    </p>
    """
  end

  attr :href, :string, required: true
  attr :size, :atom, required: true
  slot :inner_block, required: true

  defp tech_link(assigns) do
    ~H"""
    <a href={@href} target="_blank" class={link_class(@size)}>
      {render_slot(@inner_block)}
    </a>
    """
  end

  defp container_class(:sm), do: "text-purple-300/40 text-xs"
  defp container_class(:md), do: "text-purple-300/50 text-sm"

  defp link_class(:sm),
    do: "text-purple-300/60 hover:text-purple-200 underline underline-offset-2"

  defp link_class(:md), do: "text-purple-300 hover:text-purple-200 underline underline-offset-2"
end
