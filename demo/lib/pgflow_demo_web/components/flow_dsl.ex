defmodule PgflowDemoWeb.Components.FlowDSL do
  @moduledoc """
  Component for rendering the Flow DSL code with syntax highlighting.
  Supports highlighting individual steps based on flow execution status.
  """

  use Phoenix.Component

  alias PgflowDemo.Flows.{ArticleFlow, OnboardingFlow}

  # Read flow sources at compile time
  @article_source_path "lib/pgflow_demo/flows/article_flow.ex"
  @onboarding_source_path "lib/pgflow_demo/flows/onboarding_flow.ex"
  @external_resource @article_source_path
  @external_resource @onboarding_source_path

  # Define segments with their line ranges (1-indexed, inclusive)
  # Each segment is either a step (clickable) or structural code (not clickable)
  @article_segment_defs [
    %{id: :preamble, lines: 1..14, clickable: false},
    %{id: :fetch_article, lines: 16..36, clickable: true},
    %{id: :convert_to_markdown, lines: 38..51, clickable: true},
    %{id: :summarize, lines: 53..66, clickable: true},
    %{id: :extract_keywords, lines: 68..81, clickable: true},
    %{id: :publish, lines: 83..92, clickable: true}
  ]

  @onboarding_segment_defs [
    %{id: :preamble, lines: 1..22, clickable: false},
    %{id: :create_account, lines: 24..33, clickable: true},
    %{id: :setup_premium, lines: 35..43, clickable: true},
    %{id: :activate_perk, lines: 45..50, clickable: true},
    %{id: :send_welcome, lines: 52..64, clickable: true},
    %{id: :finish, lines: 66..71, clickable: true}
  ]

  @processed_article_segments (
                                flow_lines =
                                  @article_source_path |> File.read!() |> String.split("\n")

                                Enum.map(@article_segment_defs, fn segment ->
                                  code_lines =
                                    Enum.slice(
                                      flow_lines,
                                      (segment.lines.first - 1)..(segment.lines.last - 1)
                                    )

                                  code = Enum.join(code_lines, "\n")
                                  html = Makeup.highlight(code, lexer: Makeup.Lexers.ElixirLexer)

                                  Map.merge(segment, %{
                                    code: code,
                                    html: html,
                                    line_count: length(code_lines)
                                  })
                                end)
                              )

  @processed_onboarding_segments (
                                   flow_lines =
                                     @onboarding_source_path |> File.read!() |> String.split("\n")

                                   Enum.map(@onboarding_segment_defs, fn segment ->
                                     code_lines =
                                       Enum.slice(
                                         flow_lines,
                                         (segment.lines.first - 1)..(segment.lines.last - 1)
                                       )

                                     code = Enum.join(code_lines, "\n")

                                     html =
                                       Makeup.highlight(code, lexer: Makeup.Lexers.ElixirLexer)

                                     Map.merge(segment, %{
                                       code: code,
                                       html: html,
                                       line_count: length(code_lines)
                                     })
                                   end)
                                 )

  @doc """
  Returns the pre-processed ArticleFlow DSL segments for use in templates.
  """
  def get_segments, do: get_segments(ArticleFlow)

  @doc """
  Returns the pre-processed DSL segments for the given flow module.
  """
  def get_segments(ArticleFlow), do: @processed_article_segments
  def get_segments(OnboardingFlow), do: @processed_onboarding_segments

  @doc """
  Renders the Flow DSL with interactive step highlighting.

  ## Assigns
  - segments: List of DSL segments (from get_segments/0 or get_segments/1)
  - steps: Map of step_slug => status (:pending, :running, :completed, :failed, :skipped)
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
  defp status_class(:skipped, _), do: "bg-slate-500/10 opacity-60 hover:bg-slate-500/20"
  defp status_class(:pending, true), do: "hover:bg-white/5"
  defp status_class(:pending, false), do: ""
  defp status_class(nil, _), do: ""

  defp highlight_class(true), do: "ring-2 ring-pink-500"
  defp highlight_class(false), do: ""
end
