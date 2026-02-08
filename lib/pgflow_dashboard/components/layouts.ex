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
  - MobileMenu

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
            <!-- Mobile menu button -->
            <button
              type="button"
              class="sm:hidden p-2 -ml-2 rounded-md text-slate-500 hover:text-slate-700 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700 transition-colors"
              phx-click={open_mobile_menu()}
              aria-label="Open navigation menu"
              aria-expanded="false"
              aria-controls="mobile-menu"
            >
              <svg class="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            </button>

            <.link navigate={@base_path} class="flex items-center gap-2">
              <span class="text-lg font-bold text-purple-600 dark:text-purple-400">PgFlow</span>
              <span class="text-sm text-slate-500 dark:text-slate-400">Dashboard</span>
            </.link>

            <div class="hidden sm:flex items-center gap-1">
              <.nav_link
                navigate={"#{@base_path}/workers"}
                current={@current_page == :workers}
              >
                Workers
              </.nav_link>
              <.nav_link
                navigate={"#{@base_path}/runs"}
                current={@current_page == :runs}
              >
                Runs
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
                navigate={"#{@base_path}/crons"}
                current={@current_page == :crons}
              >
                Crons
              </.nav_link>
            </div>
          </div>

          <div class="flex items-center gap-1">
            <!-- Keyboard shortcuts button (hidden on mobile) -->
            <button
              type="button"
              id="shortcuts-button"
              phx-click={JS.remove_class("hidden", to: "#shortcuts-modal")}
              class="hidden sm:block p-2 rounded-md text-slate-500 hover:text-slate-700 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700 transition-colors cursor-pointer"
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

      <!-- Mobile Menu Slide-out -->
      <.mobile_menu base_path={@base_path} current_page={@current_page} />

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
                    <.shortcut_row key="g c" description="Go to Crons" />
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

  # Mobile menu slide-out drawer
  attr(:base_path, :string, required: true)
  attr(:current_page, :atom, required: true)

  defp mobile_menu(assigns) do
    ~H"""
    <div
      id="mobile-menu"
      phx-hook="MobileMenu"
      class="sm:hidden"
      aria-labelledby="mobile-menu-title"
      role="dialog"
      aria-modal="true"
    >
      <!-- Backdrop -->
      <div
        id="mobile-menu-backdrop"
        class="fixed inset-0 z-[60] bg-slate-900/50 dark:bg-slate-900/80 opacity-0 pointer-events-none transition-opacity duration-300 ease-in-out"
        phx-click={close_mobile_menu()}
        aria-hidden="true"
      >
      </div>

      <!-- Slide-out panel -->
      <div
        id="mobile-menu-panel"
        class="fixed inset-y-0 left-0 z-[70] w-72 max-w-[calc(100%-3rem)] bg-white dark:bg-slate-800 shadow-xl -translate-x-full transition-transform duration-300 ease-in-out"
      >
        <!-- Header -->
        <div class="flex items-center justify-between h-14 px-4 border-b border-slate-200 dark:border-slate-700">
          <span id="mobile-menu-title" class="text-lg font-bold text-purple-600 dark:text-purple-400">
            PgFlow
          </span>
          <button
            type="button"
            class="p-2 -mr-2 rounded-md text-slate-500 hover:text-slate-700 hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700 transition-colors"
            phx-click={close_mobile_menu()}
            aria-label="Close menu"
          >
            <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <!-- Navigation links -->
        <nav class="px-2 py-4 space-y-1">
          <.mobile_nav_link
            navigate={@base_path}
            current={@current_page == :overview}
            icon="home"
          >
            Overview
          </.mobile_nav_link>
          <.mobile_nav_link
            navigate={"#{@base_path}/workers"}
            current={@current_page == :workers}
            icon="cpu"
          >
            Workers
          </.mobile_nav_link>
          <.mobile_nav_link
            navigate={"#{@base_path}/runs"}
            current={@current_page == :runs}
            icon="play"
          >
            Runs
          </.mobile_nav_link>
          <.mobile_nav_link
            navigate={"#{@base_path}/flows"}
            current={@current_page == :flows}
            icon="workflow"
          >
            Flows
          </.mobile_nav_link>
          <.mobile_nav_link
            navigate={"#{@base_path}/jobs"}
            current={@current_page == :jobs}
            icon="briefcase"
          >
            Jobs
          </.mobile_nav_link>
          <.mobile_nav_link
            navigate={"#{@base_path}/crons"}
            current={@current_page == :crons}
            icon="clock"
          >
            Crons
          </.mobile_nav_link>
        </nav>

        <!-- Footer -->
        <div class="absolute bottom-0 left-0 right-0 px-4 py-3 border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50">
          <p class="text-xs text-slate-500 dark:text-slate-400">
            PgFlow Dashboard
          </p>
        </div>
      </div>
    </div>
    """
  end

  # Mobile navigation link with icon
  attr(:navigate, :string, required: true)
  attr(:current, :boolean, default: false)
  attr(:icon, :string, required: true)
  slot(:inner_block, required: true)

  defp mobile_nav_link(assigns) do
    ~H"""
    <.link
      navigate={@navigate}
      phx-click={close_mobile_menu()}
      class={[
        "flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors",
        @current && "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400",
        !@current && "text-slate-700 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-700"
      ]}
    >
      <.mobile_nav_icon name={@icon} />
      {render_slot(@inner_block)}
    </.link>
    """
  end

  # Icons for mobile navigation
  defp mobile_nav_icon(%{name: "home"} = assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z" />
    </svg>
    """
  end

  defp mobile_nav_icon(%{name: "cpu"} = assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z" />
    </svg>
    """
  end

  defp mobile_nav_icon(%{name: "workflow"} = assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
    </svg>
    """
  end

  defp mobile_nav_icon(%{name: "briefcase"} = assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
    </svg>
    """
  end

  defp mobile_nav_icon(%{name: "clock"} = assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
    """
  end

  defp mobile_nav_icon(%{name: "play"} = assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
    """
  end

  defp mobile_nav_icon(assigns) do
    ~H"""
    <svg class="w-5 h-5 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
    </svg>
    """
  end

  # JS commands for mobile menu animations
  defp open_mobile_menu do
    JS.remove_class("opacity-0 pointer-events-none", to: "#mobile-menu-backdrop")
    |> JS.add_class("opacity-100 pointer-events-auto", to: "#mobile-menu-backdrop")
    |> JS.remove_class("-translate-x-full", to: "#mobile-menu-panel")
    |> JS.add_class("translate-x-0", to: "#mobile-menu-panel")
    |> JS.dispatch("menu:opened", to: "#mobile-menu")
  end

  defp close_mobile_menu do
    JS.add_class("opacity-0 pointer-events-none", to: "#mobile-menu-backdrop")
    |> JS.remove_class("opacity-100 pointer-events-auto", to: "#mobile-menu-backdrop")
    |> JS.add_class("-translate-x-full", to: "#mobile-menu-panel")
    |> JS.remove_class("translate-x-0", to: "#mobile-menu-panel")
    |> JS.dispatch("menu:closed", to: "#mobile-menu")
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
