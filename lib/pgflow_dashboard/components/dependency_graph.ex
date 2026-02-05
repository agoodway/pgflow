defmodule PgFlowDashboard.Components.DependencyGraph do
  @moduledoc """
  Server-rendered SVG DAG visualization component.

  Renders a directed acyclic graph showing flow step dependencies.
  """

  use Phoenix.Component

  @node_radius 24
  @node_spacing_x 160
  @node_spacing_y 80
  @padding 40

  @doc """
  Renders a dependency graph for a flow.

  ## Attributes

    * `:steps` - List of step maps with :step_slug and :deps keys
    * `:step_states` - Optional map of step_slug => status for coloring
    * `:highlighted_step` - Optional step slug to highlight

  """
  attr(:steps, :list, required: true)
  attr(:step_states, :map, default: %{})
  attr(:highlighted_step, :any, default: nil)
  attr(:on_click, :any, default: nil)

  def dependency_graph(assigns) do
    {nodes, edges, width, height} = layout_graph(assigns.steps)

    assigns =
      assigns
      |> assign(:nodes, nodes)
      |> assign(:edges, edges)
      |> assign(:width, width)
      |> assign(:height, height)
      |> assign(:node_radius, @node_radius)

    ~H"""
    <svg
      viewBox={"0 0 #{@width} #{@height}"}
      class="w-full h-auto max-w-2xl mx-auto"
      role="img"
      aria-label="Flow dependency graph"
    >
      <defs>
        <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8" />
        </marker>
        <marker id="arrowhead-active" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
          <polygon points="0 0, 10 3.5, 0 7" fill="#8b5cf6" />
        </marker>
      </defs>

      <!-- Edges -->
      <%= for edge <- @edges do %>
        <.edge
          edge={edge}
          step_states={@step_states}
          node_radius={@node_radius}
        />
      <% end %>

      <!-- Nodes -->
      <%= for graph_node <- @nodes do %>
        <.graph_node
          node={graph_node}
          status={Map.get(@step_states, graph_node.slug)}
          highlighted={@highlighted_step == graph_node.slug}
          node_radius={@node_radius}
          on_click={@on_click}
        />
      <% end %>
    </svg>
    """
  end

  attr(:edge, :map, required: true)
  attr(:step_states, :map, required: true)
  attr(:node_radius, :integer, required: true)

  defp edge(assigns) do
    from_status = Map.get(assigns.step_states, assigns.edge.from)
    is_active = from_status in [:started, "started"]

    # Calculate line endpoints with offset for node radius
    {x1, y1} = {assigns.edge.from_x, assigns.edge.from_y}
    {x2, y2} = {assigns.edge.to_x, assigns.edge.to_y}

    dx = x2 - x1
    dy = y2 - y1
    dist = :math.sqrt(dx * dx + dy * dy)

    # Offset from center by node radius
    start_x = x1 + dx / dist * assigns.node_radius
    start_y = y1 + dy / dist * assigns.node_radius
    end_x = x2 - dx / dist * (assigns.node_radius + 8)
    end_y = y2 - dy / dist * (assigns.node_radius + 8)

    assigns =
      assigns
      |> assign(:start_x, start_x)
      |> assign(:start_y, start_y)
      |> assign(:end_x, end_x)
      |> assign(:end_y, end_y)
      |> assign(:is_active, is_active)
      |> assign(:from_completed, from_status in [:completed, "completed"])

    ~H"""
    <line
      x1={@start_x}
      y1={@start_y}
      x2={@end_x}
      y2={@end_y}
      stroke={edge_color(@from_completed, @is_active)}
      stroke-width={if @is_active, do: "2", else: "1.5"}
      stroke-dasharray={if @is_active, do: "5,5", else: "none"}
      marker-end={if @is_active, do: "url(#arrowhead-active)", else: "url(#arrowhead)"}
      class={if @is_active, do: "animate-dash", else: ""}
    />
    """
  end

  defp edge_color(true, _), do: "#10b981"
  defp edge_color(_, true), do: "#8b5cf6"
  defp edge_color(_, _), do: "#94a3b8"

  attr(:node, :map, required: true)
  attr(:status, :any, default: nil)
  attr(:highlighted, :boolean, default: false)
  attr(:node_radius, :integer, required: true)
  attr(:on_click, :any, default: nil)

  defp graph_node(assigns) do
    ~H"""
    <g
      class="cursor-pointer"
      role="button"
      aria-label={"Step: #{@node.label}"}
      phx-click={@on_click}
      phx-value-step={@node.slug}
    >
      <!-- Highlight ring -->
      <circle
        :if={@highlighted}
        cx={@node.x}
        cy={@node.y}
        r={@node_radius + 4}
        fill="none"
        stroke="#8b5cf6"
        stroke-width="2"
      />

      <!-- Node circle -->
      <circle
        cx={@node.x}
        cy={@node.y}
        r={@node_radius}
        fill={node_fill(@status)}
        stroke={node_stroke(@status)}
        stroke-width="2"
        class={node_animation(@status)}
      />

      <!-- Status icon -->
      <.status_icon status={@status} x={@node.x} y={@node.y} />

      <!-- Label background -->
      <rect
        x={@node.x - String.length(@node.label) * 4 - 4}
        y={@node.y + @node_radius + 4}
        width={String.length(@node.label) * 8 + 8}
        height="18"
        fill="white"
        fill-opacity="0.9"
        rx="4"
        class="dark:fill-slate-800"
      />

      <!-- Label -->
      <text
        x={@node.x}
        y={@node.y + @node_radius + 16}
        text-anchor="middle"
        class="text-xs font-medium fill-slate-700 dark:fill-slate-300"
      >
        {@node.label}
      </text>
    </g>
    """
  end

  defp status_icon(%{status: status} = assigns) when status in [:completed, "completed"] do
    ~H"""
    <text x={@x} y={@y + 1} text-anchor="middle" dominant-baseline="middle" class="text-sm fill-white font-bold">
      ✓
    </text>
    """
  end

  defp status_icon(%{status: status} = assigns) when status in [:failed, "failed"] do
    ~H"""
    <text x={@x} y={@y + 1} text-anchor="middle" dominant-baseline="middle" class="text-sm fill-white font-bold">
      ✗
    </text>
    """
  end

  defp status_icon(%{status: status} = assigns) when status in [:started, "started"] do
    ~H"""
    <circle
      cx={@x}
      cy={@y}
      r="6"
      fill="none"
      stroke="white"
      stroke-width="2"
      stroke-dasharray="4,4"
    >
      <animateTransform
        attributeName="transform"
        type="rotate"
        from={"0 #{@x} #{@y}"}
        to={"360 #{@x} #{@y}"}
        dur="1s"
        repeatCount="indefinite"
      />
    </circle>
    """
  end

  defp status_icon(assigns) do
    ~H"""
    """
  end

  defp node_fill(status) do
    case status do
      s when s in [:completed, "completed"] -> "#10b981"
      s when s in [:failed, "failed"] -> "#ef4444"
      s when s in [:started, "started"] -> "#8b5cf6"
      _ -> "#94a3b8"
    end
  end

  defp node_stroke(status) do
    case status do
      s when s in [:completed, "completed"] -> "#059669"
      s when s in [:failed, "failed"] -> "#dc2626"
      s when s in [:started, "started"] -> "#7c3aed"
      _ -> "#64748b"
    end
  end

  defp node_animation(_status) do
    # No animation on the node itself - the spinning icon inside indicates activity
    ""
  end

  # Graph layout algorithm
  defp layout_graph([_ | _] = steps) do
    # Build dependency map
    dep_map =
      Map.new(steps, fn step ->
        slug = step[:step_slug] || step["step_slug"]
        deps = step[:deps] || step["deps"] || []
        {slug, deps}
      end)

    # Calculate levels using topological sort
    levels = calculate_levels(dep_map)

    # Group steps by level
    level_groups =
      steps
      |> Enum.group_by(fn step ->
        slug = step[:step_slug] || step["step_slug"]
        Map.get(levels, slug, 0)
      end)

    # Position nodes
    max_level = levels |> Map.values() |> Enum.max(fn -> 0 end)
    max_width = level_groups |> Map.values() |> Enum.map(&length/1) |> Enum.max(fn -> 1 end)

    nodes =
      Enum.flat_map(level_groups, fn {level, level_steps} ->
        count = length(level_steps)

        level_steps
        |> Enum.with_index()
        |> Enum.map(fn {step, idx} ->
          slug = step[:step_slug] || step["step_slug"]
          x = @padding + @node_spacing_x * level
          y = @padding + @node_spacing_y * idx + @node_spacing_y * (max_width - count) / 2

          %{
            slug: slug,
            label: format_label(slug),
            x: x,
            y: y
          }
        end)
      end)

    # Create node position map
    node_positions = Map.new(nodes, fn n -> {n.slug, {n.x, n.y}} end)

    # Build edges
    edges =
      Enum.flat_map(steps, fn step ->
        to_slug = step[:step_slug] || step["step_slug"]
        deps = step[:deps] || step["deps"] || []

        Enum.map(deps, fn from_slug ->
          {from_x, from_y} = Map.get(node_positions, from_slug, {0, 0})
          {to_x, to_y} = Map.get(node_positions, to_slug, {0, 0})

          %{
            from: from_slug,
            to: to_slug,
            from_x: from_x,
            from_y: from_y,
            to_x: to_x,
            to_y: to_y
          }
        end)
      end)

    width = @padding * 2 + @node_spacing_x * max_level + @node_radius * 2
    height = @padding * 2 + @node_spacing_y * (max_width - 1) + @node_radius * 2

    {nodes, edges, max(width, 200), max(height, 100)}
  end

  defp layout_graph(_), do: {[], [], 200, 100}

  defp calculate_levels(dep_map) do
    # Initialize all nodes at level 0
    initial = Map.new(Map.keys(dep_map), fn k -> {k, 0} end)

    # Iteratively calculate levels based on dependencies
    Enum.reduce(1..100, initial, fn _iteration, levels ->
      Enum.reduce(dep_map, levels, &update_node_level/2)
    end)
  end

  defp update_node_level({_node, []}, acc), do: acc

  defp update_node_level({node, deps}, acc) do
    max_dep_level = deps |> Enum.map(&Map.get(acc, &1, 0)) |> Enum.max(fn -> 0 end)
    Map.put(acc, node, max_dep_level + 1)
  end

  defp format_label(slug) when is_binary(slug) do
    slug
    |> String.split("_")
    |> Enum.map_join(" ", &String.capitalize/1)
    |> String.slice(0..15)
  end

  defp format_label(slug) when is_atom(slug), do: slug |> to_string() |> format_label()
  defp format_label(_), do: "?"
end
