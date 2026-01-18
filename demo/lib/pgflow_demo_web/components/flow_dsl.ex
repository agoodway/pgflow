defmodule PgflowDemoWeb.Components.FlowDSL do
  @moduledoc """
  Component for rendering the Flow DSL code with syntax highlighting.
  Supports highlighting individual steps based on flow execution status.
  """

  use Phoenix.Component

  # Read the flow source at compile time
  @flow_source_path "lib/pgflow_demo/flows/article_flow.ex"
  @external_resource @flow_source_path

  # Define segments with their line ranges (1-indexed, inclusive)
  # Each segment is either a step (clickable) or structural code (not clickable)
  @segments [
    %{id: :preamble, lines: 1..14, clickable: false},
    %{id: :fetch_article, lines: 16..36, clickable: true},
    %{id: :convert_to_markdown, lines: 38..51, clickable: true},
    %{id: :summarize, lines: 53..66, clickable: true},
    %{id: :extract_keywords, lines: 68..81, clickable: true},
    %{id: :publish, lines: 83..92, clickable: true}
  ]

  # Pre-process segments at compile time
  @flow_lines File.read!(@flow_source_path) |> String.split("\n")

  @processed_segments Enum.map(@segments, fn segment ->
                        # Extract lines for this segment (convert to 0-indexed for Enum.slice)
                        code_lines =
                          Enum.slice(
                            @flow_lines,
                            (segment.lines.first - 1)..(segment.lines.last - 1)
                          )

                        code = Enum.join(code_lines, "\n")

                        # Generate highlighted HTML using Makeup
                        html = Makeup.highlight(code, lexer: Makeup.Lexers.ElixirLexer)

                        Map.merge(segment, %{
                          code: code,
                          html: html,
                          line_count: length(code_lines)
                        })
                      end)

  @doc """
  Returns the pre-processed DSL segments for use in templates.
  """
  def get_segments, do: @processed_segments

  @doc """
  Renders the Flow DSL with interactive step highlighting.

  ## Assigns
  - segments: List of DSL segments (from get_segments/0)
  - steps: Map of step_slug => status (:pending, :running, :completed, :failed)
  - highlighted_step: Currently highlighted step slug (atom) or nil
  """
  attr :segments, :list, required: true
  attr :steps, :map, required: true
  attr :highlighted_step, :atom, default: nil

  def flow_dsl(assigns) do
    ~H"""
    <div class="font-mono text-xs leading-relaxed">
      <%= for segment <- @segments do %>
        <% status = if segment.clickable, do: Map.get(@steps, segment.id, :pending), else: nil %>
        <% is_highlighted = segment.id == @highlighted_step %>
        <div
          id={"dsl-segment-#{segment.id}"}
          class={segment_classes(segment.clickable, status, is_highlighted)}
          phx-click={if segment.clickable, do: "click_dsl_step"}
          phx-value-step={if segment.clickable, do: to_string(segment.id)}
        >
          {Phoenix.HTML.raw(segment.html)}
        </div>
      <% end %>
    </div>
    """
  end

  defp segment_classes(clickable, status, is_highlighted) do
    base = "px-3 py-1 -mx-3 rounded transition-all duration-200"
    clickable_class = clickable_class(clickable)
    status_class = status_class(status, clickable)
    highlight_class = highlight_class(is_highlighted)

    Enum.join([base, clickable_class, status_class, highlight_class], " ")
  end

  defp clickable_class(true), do: "cursor-pointer"
  defp clickable_class(false), do: ""

  defp status_class(:running, _), do: "bg-purple-500/20"
  defp status_class(:completed, _), do: "bg-emerald-500/10 hover:bg-emerald-500/20"
  defp status_class(:failed, _), do: "bg-red-500/10"
  defp status_class(:pending, true), do: "hover:bg-white/5"
  defp status_class(:pending, false), do: ""
  defp status_class(nil, _), do: ""

  defp highlight_class(true), do: "ring-2 ring-pink-500"
  defp highlight_class(false), do: ""
end
