defmodule PgflowDemoWeb.Components.ScenarioSource do
  @moduledoc """
  Renders highlighted source for a scenario's flow or job module.
  """

  use Phoenix.Component

  @sources Map.new(PgflowDemo.Scenarios.list(), fn descriptor ->
             module = descriptor.module

             source =
               if module do
                 Code.ensure_compiled!(module)
                 path = module.__info__(:compile)[:source] |> to_string()
                 @external_resource path
                 File.read!(path)
               end

             {module, source}
           end)

  @doc """
  Renders the module source for the selected scenario definition.
  """
  attr :module, :any, default: nil
  attr :walkthrough, :string, default: nil
  attr :id, :string, default: "scenario-source"

  def scenario_source(assigns) do
    assigns =
      assigns
      |> assign(:source_html, source_html(assigns.module))
      |> assign(:source_path, inspect(assigns.module))

    ~H"""
    <section id={@id} class="space-y-2">
      <h2 class="text-lg font-semibold">Source</h2>
      <%= if @walkthrough do %>
        <p class="text-sm text-base-content/70">{@walkthrough}</p>
      <% else %>
        <%= if @source_html do %>
          <p class="text-xs font-mono text-base-content/50">{@source_path}</p>
          <div class="overflow-x-auto rounded-lg bg-base-200 p-4 text-xs font-mono leading-relaxed">
            {Phoenix.HTML.raw(@source_html)}
          </div>
        <% else %>
          <p class="text-sm text-base-content/70">No runnable module for this walkthrough.</p>
        <% end %>
      <% end %>
    </section>
    """
  end

  defp source_html(nil), do: nil

  defp source_html(module) when is_atom(module) do
    case Map.get(@sources, module) do
      nil -> nil
      source -> Makeup.highlight(source, lexer: Makeup.Lexers.ElixirLexer)
    end
  end
end
