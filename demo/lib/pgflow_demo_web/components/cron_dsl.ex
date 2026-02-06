defmodule PgflowDemoWeb.Components.CronDSL do
  @moduledoc """
  Component for rendering the Cron DSL code with syntax highlighting
  and next run countdown.
  """

  use Phoenix.Component

  # Read the cron source at compile time
  @cron_source_path "lib/pgflow_demo/crons/article_flow_cleanup.ex"
  @external_resource @cron_source_path

  @cron_source File.read!(@cron_source_path)
  @highlighted_source Makeup.highlight(@cron_source, lexer: Makeup.Lexers.ElixirLexer)

  @doc """
  Returns the pre-processed cron source code.
  """
  @spec get_source() :: String.t()
  def get_source, do: @cron_source

  @doc """
  Returns the syntax highlighted HTML.
  """
  @spec get_highlighted_source() :: String.t()
  def get_highlighted_source, do: @highlighted_source

  @doc """
  Calculates when the cron will next run based on the expression "0 * * * *" (hourly at minute 0).
  """
  @spec get_next_run_info() :: %{
          next_run_at: DateTime.t(),
          relative_time: String.t(),
          expression: String.t(),
          human_schedule: String.t()
        }
  def get_next_run_info do
    now = DateTime.utc_now()
    current_minute = now.minute

    # For "0 * * * *" - runs at minute 0 of every hour
    next_run_at =
      if current_minute == 0 do
        # Already at minute 0, next run is in 1 hour
        DateTime.add(now, 3600, :second) |> DateTime.truncate(:second)
      else
        # Calculate seconds until next hour
        seconds_to_next_hour = (60 - current_minute) * 60 - now.second
        DateTime.add(now, seconds_to_next_hour, :second) |> DateTime.truncate(:second)
      end

    %{
      next_run_at: next_run_at,
      relative_time: Timex.from_now(next_run_at),
      expression: "0 * * * *",
      human_schedule: "Hourly"
    }
  end

  @doc """
  Renders the Cron DSL with info card.
  """
  attr :highlighted_source, :string, required: true
  attr :next_run_info, :map, required: true

  def cron_dsl(assigns) do
    ~H"""
    <div class="space-y-4">
      <!-- Cron Info Card -->
      <div class="flex items-center gap-6 px-4 py-3 bg-slate-800/50 rounded-xl border border-amber-500/20">
        <div class="flex items-center gap-2">
          <span class="text-amber-400 text-lg">⏰</span>
          <div>
            <p class="text-sm font-medium text-white">{@next_run_info.human_schedule}</p>
            <p class="text-xs text-slate-400 font-mono">{@next_run_info.expression}</p>
          </div>
        </div>
        <div class="h-8 w-px bg-slate-700"></div>
        <div>
          <p class="text-xs text-slate-400">Next run</p>
          <p class="text-sm font-semibold text-amber-400">{@next_run_info.relative_time}</p>
        </div>
        <div class="h-8 w-px bg-slate-700"></div>
        <div>
          <p class="text-xs text-slate-400">Retention</p>
          <p class="text-sm font-medium text-slate-300">24 hours</p>
        </div>
        <div class="h-8 w-px bg-slate-700"></div>
        <div class="flex items-center gap-1.5">
          <span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
          <span class="text-xs text-emerald-400">Active</span>
        </div>
      </div>

    <!-- Cron Source Code -->
      <div class="font-mono text-xs leading-relaxed">
        {Phoenix.HTML.raw(@highlighted_source)}
      </div>
    </div>
    """
  end
end
