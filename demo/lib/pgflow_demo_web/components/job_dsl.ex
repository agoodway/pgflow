defmodule PgflowDemoWeb.Components.JobDSL do
  @moduledoc """
  Component for rendering the SendEmail Job DSL with syntax highlighting.
  """

  use Phoenix.Component

  @job_source_path "lib/pgflow_demo/jobs/send_email.ex"
  @external_resource @job_source_path

  @job_source File.read!(@job_source_path)
  @highlighted_source Makeup.highlight(@job_source, lexer: Makeup.Lexers.ElixirLexer)

  @spec get_highlighted_source() :: String.t()
  def get_highlighted_source, do: @highlighted_source

  attr :highlighted_source, :string, required: true

  def job_dsl(assigns) do
    ~H"""
    <div class="font-mono text-xs leading-relaxed">
      {Phoenix.HTML.raw(@highlighted_source)}
    </div>
    """
  end
end
