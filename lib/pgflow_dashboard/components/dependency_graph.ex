defmodule PgFlowDashboard.Components.DependencyGraph do
  @moduledoc """
  Server-rendered SVG DAG visualization component.

  Renders a directed acyclic graph showing flow step dependencies.
  """

  use Phoenix.Component

  @node_radius 24
  @node_spacing_y 96
  @level_gap 48
  @padding 40
  @padding_top 64
  @label_char_width 8

  @doc """
  Adds each step's dependency slugs for dependency-graph rendering.
  """
  @spec with_dependencies([PgFlow.Schema.Step.t()], [PgFlow.Schema.Dep.t()]) :: [map()]
  def with_dependencies(steps, deps) do
    deps_by_step = Enum.group_by(deps, & &1.step_slug, & &1.dep_slug)

    Enum.map(steps, fn step ->
      %{step_slug: step.step_slug, deps: Map.get(deps_by_step, step.step_slug, [])}
    end)
  end

  @doc """
  Renders a dependency graph for a flow.

  ## Attributes

    * `:steps` - List of step maps with :step_slug and :deps keys
    * `:step_states` - Optional map of step_slug => status for coloring
    * `:highlighted_step` - Optional step slug to highlight

  """
  attr(:steps, :list, required: true)
  attr(:id, :string, default: "flow-dependency-graph")
  attr(:step_states, :map, default: %{})
  attr(:highlighted_step, :any, default: nil)
  attr(:on_click, :any, default: nil)
  attr(:on_keydown, :any, default: nil)

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
    <%= if @nodes == [] do %>
      <div class="rounded-md border border-dashed border-slate-300 bg-slate-50 px-4 py-8 text-center text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-900/50 dark:text-slate-400">
        No workflow steps
      </div>
    <% else %>
      <div
        id={@id}
        phx-hook={@on_click && "GraphNodeKeyboard"}
        class="overflow-x-auto"
        role="region"
        tabindex="0"
        aria-label="Scrollable flow dependency graph"
      >
        <svg
          width={@width}
          height={@height}
          viewBox={"0 0 #{@width} #{@height}"}
          class="block h-auto max-w-none mx-auto"
          role="group"
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
              on_keydown={@on_keydown}
            />
          <% end %>
        </svg>
      </div>
    <% end %>
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
  attr(:on_keydown, :any, default: nil)

  defp graph_node(assigns) do
    assigns =
      assigns
      |> assign(:status_text, status_label(assigns.status))
      |> assign(:tooltip_width, max(String.length(status_label(assigns.status)) * 7 + 24, 64))

    ~H"""
    <g
      class={[@on_click && "group cursor-pointer focus:outline-none"]}
      role={@on_click && "button"}
      tabindex={@on_click && "0"}
      aria-label={"Step: #{@node.label}, #{status_label(@status)}"}
      aria-pressed={aria_pressed(@on_click, @highlighted)}
      phx-click={@on_click}
      phx-keydown={@on_keydown}
      phx-value-step={@node.slug}
    >
      <circle
        :if={@on_click}
        cx={@node.x}
        cy={@node.y}
        r={@node_radius + 5}
        fill="none"
        stroke-width="3"
        class="stroke-transparent group-focus-visible:stroke-purple-500"
      />

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
        stroke-width="2"
        class={[node_color_classes(@status), node_animation(@status)]}
      />

      <!-- Status icon -->
      <.status_icon status={@status} x={@node.x} y={@node.y} />

      <!-- Status tooltip -->
      <g
        aria-hidden="true"
        class="pointer-events-none opacity-0 transition-opacity group-hover:opacity-100 group-focus-visible:opacity-100"
      >
        <rect
          x={@node.x - @tooltip_width / 2}
          y={@node.y - @node_radius - 32}
          width={@tooltip_width}
          height="24"
          rx="6"
          class="fill-slate-900 dark:fill-slate-100"
        />
        <text
          x={@node.x}
          y={@node.y - @node_radius - 16}
          text-anchor="middle"
          class="text-xs font-semibold fill-white dark:fill-slate-900"
        >
          {@status_text}
        </text>
      </g>

      <!-- Label background -->
      <rect
        x={@node.x - @node.label_width / 2}
        y={@node.y + @node_radius + 4}
        width={@node.label_width}
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
        class="text-xs font-mono font-medium fill-slate-700 dark:fill-slate-300"
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

  defp status_icon(%{status: status} = assigns) when status in [:skipped, "skipped"] do
    ~H"""
    <text x={@x} y={@y + 1} text-anchor="middle" dominant-baseline="middle" class="text-sm fill-white font-bold">
      –
    </text>
    """
  end

  defp status_icon(assigns) do
    ~H"""
    """
  end

  defp node_color_classes(s) when s in [:completed, "completed"],
    do: "fill-emerald-500 stroke-emerald-600"

  defp node_color_classes(s) when s in [:failed, "failed"],
    do: "fill-red-500 stroke-red-600"

  defp node_color_classes(s) when s in [:started, "started"],
    do: "fill-violet-500 stroke-violet-600"

  defp node_color_classes(s) when s in [:skipped, "skipped"],
    do: "fill-orange-600 stroke-orange-800 dark:fill-amber-400 dark:stroke-amber-200"

  defp node_color_classes(_), do: "fill-slate-400 stroke-slate-500"

  defp node_animation(_status) do
    # No animation on the node itself - the spinning icon inside indicates activity
    ""
  end

  defp status_label(status) when status in [:completed, "completed"], do: "Completed"
  defp status_label(status) when status in [:failed, "failed"], do: "Failed"
  defp status_label(status) when status in [:started, "started"], do: "Running"
  defp status_label(status) when status in [:skipped, "skipped"], do: "Skipped"
  defp status_label(_status), do: "Pending"

  defp aria_pressed(nil, _highlighted), do: nil
  defp aria_pressed(_on_click, highlighted), do: to_string(highlighted)

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

    labels =
      Map.new(steps, fn step ->
        slug = step[:step_slug] || step["step_slug"]
        {slug, format_label(slug)}
      end)

    # Position nodes with enough room for the longest label in each level.
    max_level = levels |> Map.values() |> Enum.max(fn -> 0 end)
    max_width = level_groups |> Map.values() |> Enum.map(&length/1) |> Enum.max(fn -> 1 end)

    level_widths =
      Map.new(0..max_level, fn level ->
        width =
          level_groups
          |> Map.get(level, [])
          |> Enum.map(fn step ->
            slug = step[:step_slug] || step["step_slug"]
            label_width(Map.fetch!(labels, slug))
          end)
          |> Enum.max(fn -> 120 end)
          |> max(120)

        {level, width}
      end)

    {level_centers, graph_content_width} = level_centers(level_widths, max_level)

    nodes =
      Enum.flat_map(level_groups, fn {level, level_steps} ->
        count = length(level_steps)

        level_steps
        |> Enum.with_index()
        |> Enum.map(fn {step, idx} ->
          slug = step[:step_slug] || step["step_slug"]
          label = Map.fetch!(labels, slug)
          x = Map.fetch!(level_centers, level)
          y = @padding_top + @node_spacing_y * idx + @node_spacing_y * (max_width - count) / 2

          %{
            slug: slug,
            label: label,
            label_width: label_width(label),
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

    width = @padding * 2 + graph_content_width
    height = @padding_top + @node_spacing_y * (max_width - 1) + @node_radius + 50

    {nodes, edges, max(width, 240), max(height, 138)}
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

  defp level_centers(level_widths, max_level) do
    {centers, cursor} =
      Enum.reduce(0..max_level, {%{}, 0}, fn level, {centers, cursor} ->
        width = Map.fetch!(level_widths, level)
        center = cursor + width / 2
        {Map.put(centers, level, @padding + center), cursor + width + @level_gap}
      end)

    {centers, cursor - @level_gap}
  end

  defp label_width(label), do: String.length(label) * @label_char_width + 16

  defp format_label(slug) when is_binary(slug) do
    slug
    |> String.split("_")
    |> Enum.map_join(" ", &String.capitalize/1)
  end

  defp format_label(slug) when is_atom(slug), do: slug |> to_string() |> format_label()
  defp format_label(_), do: "?"
end
