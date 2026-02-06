defmodule PgFlowDashboard.Components.Layouts do
  @moduledoc """
  Layout components for the PgFlow Dashboard.

  See `PgFlowDashboard.Hooks` for information on installing the JavaScript hooks
  required for interactive features (dark mode, keyboard shortcuts).
  """

  use Phoenix.Component

  alias Phoenix.LiveView.JS

  @doc """
  Dashboard layout with sidebar navigation.

  Requires the following hooks to be registered with LiveSocket:
  - DarkMode
  - KeyboardShortcuts
  - ShortcutsModal

  See `PgFlowDashboard.Hooks` for installation instructions.
  """
  attr(:current_page, :atom, required: true)
  attr(:base_path, :string, default: "/pgflow")
  slot(:inner_block, required: true)

  def dashboard_layout(assigns) do
    ~H"""
    <div
      id="keyboard-shortcuts"
      phx-hook="KeyboardShortcuts"
      class="min-h-screen bg-slate-50 dark:bg-slate-900"
      data-base-path={@base_path}
    >
      <nav class="fixed top-0 left-0 right-0 z-50 h-14 bg-white dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700">
        <div class="h-full px-4 flex items-center justify-between">
          <div class="flex items-center gap-6">
            <.link navigate={@base_path} class="flex items-center gap-2">
              <span class="text-lg font-bold text-purple-600 dark:text-purple-400">PgFlow</span>
              <span class="text-sm text-slate-500 dark:text-slate-400">Dashboard</span>
            </.link>

            <div class="hidden sm:flex items-center gap-1">
              <.nav_link
                navigate={"#{@base_path}"}
                current={@current_page == :overview}
              >
                Overview
              </.nav_link>
              <.nav_link
                navigate={"#{@base_path}/workers"}
                current={@current_page == :workers}
              >
                Workers
              </.nav_link>
              <.nav_link
                navigate={"#{@base_path}/flows"}
                current={@current_page == :flows}
              >
                Flows
              </.nav_link>
              <.nav_link
                navigate={"#{@base_path}/jobs"}
                current={@current_page == :jobs}
              >
                Jobs
              </.nav_link>
              <.nav_link
                navigate={"#{@base_path}/runs"}
                current={@current_page == :runs}
              >
                Runs
              </.nav_link>
            </div>
          </div>

          <div class="flex items-center gap-1">
            <!-- Keyboard shortcuts button -->
            <button
              type="button"
              id="shortcuts-button"
              phx-click={JS.remove_class("hidden", to: "#shortcuts-modal")}
              class="p-2 rounded-md text-slate-500 hover:text-slate-700 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700 transition-colors cursor-pointer"
              aria-label="Keyboard shortcuts"
              title="Keyboard Shortcuts"
            >
              <span class="flex items-center justify-center w-5 h-5 text-xs font-semibold border border-current rounded">K</span>
            </button>

            <!-- Dark mode toggle -->
            <button
              type="button"
              id="dark-mode-toggle"
              phx-hook="DarkMode"
              class="p-2 rounded-md text-slate-500 hover:text-slate-700 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700 transition-colors cursor-pointer"
              aria-label="Toggle dark mode"
            >
              <svg class="w-5 h-5 hidden dark:block" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
              </svg>
              <svg class="w-5 h-5 block dark:hidden" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
              </svg>
            </button>
          </div>
        </div>
      </nav>

      <main class="pt-14 min-h-screen">
        <div class="max-w-7xl mx-auto px-4 py-6">
          {render_slot(@inner_block)}
        </div>
      </main>

      <!-- Keyboard Shortcuts Modal -->
      <div
        id="shortcuts-modal"
        phx-hook="ShortcutsModal"
        class="hidden fixed inset-0 z-[100] overflow-y-auto"
        aria-labelledby="modal-title"
        role="dialog"
        aria-modal="true"
      >
        <div class="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:p-0">
          <!-- Background overlay -->
          <div
            class="fixed inset-0 bg-slate-900/50 dark:bg-slate-900/75 transition-opacity"
            phx-click={JS.add_class("hidden", to: "#shortcuts-modal")}
          ></div>

          <!-- Modal panel -->
          <div class="relative bg-white dark:bg-slate-800 rounded-lg text-left shadow-xl transform transition-all sm:my-8 sm:max-w-lg sm:w-full border border-slate-200 dark:border-slate-700">
            <div class="px-6 py-4 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
              <h3 class="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2" id="modal-title">
                <span class="flex items-center justify-center w-6 h-6 text-sm font-semibold border border-slate-400 dark:border-slate-500 rounded text-slate-600 dark:text-slate-300">K</span>
                Keyboard Shortcuts
              </h3>
              <button
                type="button"
                phx-click={JS.add_class("hidden", to: "#shortcuts-modal")}
                class="p-1 rounded-md text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 transition-colors"
              >
                <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            <div class="px-6 py-4">
              <div class="space-y-4">
                <div>
                  <h4 class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Navigation</h4>
                  <div class="space-y-2">
                    <.shortcut_row key="g o" description="Go to Overview" />
                    <.shortcut_row key="g w" description="Go to Workers" />
                    <.shortcut_row key="g f" description="Go to Flows" />
                    <.shortcut_row key="g j" description="Go to Jobs" />
                    <.shortcut_row key="g r" description="Go to Runs" />
                  </div>
                </div>

                <div>
                  <h4 class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Actions</h4>
                  <div class="space-y-2">
                    <.shortcut_row key="? or K" description="Show this help" />
                    <.shortcut_row key="d" description="Toggle dark mode" />
                    <.shortcut_row key="Esc" description="Close modal / Clear selection" />
                  </div>
                </div>

                <div>
                  <h4 class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">On Detail Views</h4>
                  <div class="space-y-2">
                    <.shortcut_row key="j" description="Next step (run detail)" />
                    <.shortcut_row key="k" description="Previous step (run detail)" />
                    <.shortcut_row key="]" description="Next record (newer)" />
                    <.shortcut_row key="[" description="Previous record (older)" />
                  </div>
                </div>
              </div>
            </div>

            <div class="px-6 py-3 bg-slate-50 dark:bg-slate-800/50 border-t border-slate-200 dark:border-slate-700 rounded-b-lg">
              <p class="text-xs text-slate-500 dark:text-slate-400">
                Press <kbd class="px-1.5 py-0.5 bg-slate-200 dark:bg-slate-700 rounded text-xs font-mono">?</kbd> or <kbd class="px-1.5 py-0.5 bg-slate-200 dark:bg-slate-700 rounded text-xs font-mono">K</kbd> anytime to show shortcuts
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
  end

  @doc """
  Navigation link component.
  """
  attr(:navigate, :string, required: true)
  attr(:current, :boolean, default: false)
  slot(:inner_block, required: true)

  def nav_link(assigns) do
    ~H"""
    <.link
      navigate={@navigate}
      class={[
        "px-3 py-2 text-sm font-medium rounded-md transition-colors",
        @current && "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400",
        !@current && "text-slate-600 hover:text-slate-900 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700"
      ]}
    >
      {render_slot(@inner_block)}
    </.link>
    """
  end

  @doc """
  Page header component.
  """
  attr(:title, :string, required: true)
  attr(:subtitle, :string, default: nil)
  slot(:actions)

  def page_header(assigns) do
    ~H"""
    <div class="mb-6 flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-bold text-slate-900 dark:text-white">{@title}</h1>
        <p :if={@subtitle} class="mt-1 text-sm text-slate-500 dark:text-slate-400">{@subtitle}</p>
      </div>
      <div :if={@actions != []} class="flex items-center gap-2">
        {render_slot(@actions)}
      </div>
    </div>
    """
  end

  # Keyboard shortcut row for the help modal.
  attr(:key, :string, required: true)
  attr(:description, :string, required: true)

  defp shortcut_row(assigns) do
    keys = String.split(assigns.key, " ")
    assigns = assign(assigns, :keys, keys)

    ~H"""
    <div class="flex items-center justify-between">
      <span class="text-sm text-slate-600 dark:text-slate-400">{@description}</span>
      <div class="flex items-center gap-1">
        <%= for key <- @keys do %>
          <kbd class="px-2 py-1 bg-slate-100 dark:bg-slate-700 border border-slate-300 dark:border-slate-600 rounded text-xs font-mono text-slate-700 dark:text-slate-300">
            {key}
          </kbd>
        <% end %>
      </div>
    </div>
    """
  end
end
