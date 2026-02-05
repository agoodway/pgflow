defmodule PgFlowDashboard.Hooks do
  @moduledoc """
  JavaScript hooks for PgFlow Dashboard.

  PgFlow Dashboard uses LiveView hooks for interactive features like dark mode
  toggling and keyboard shortcuts. These hooks must be registered with your
  LiveSocket for the dashboard to function properly.

  ## Installation

  ### Option 1: Import from the package (recommended)

  Add to your `assets/js/app.js`:

      import { DarkMode, KeyboardShortcuts, ShortcutsModal } from "../../deps/pgflow/priv/static/pgflow_dashboard/hooks"

      let liveSocket = new LiveSocket("/live", Socket, {
        hooks: { DarkMode, KeyboardShortcuts, ShortcutsModal, ...YourOtherHooks }
      })

  ### Option 2: Copy the hooks

  Copy the hook files from `deps/pgflow/priv/static/pgflow_dashboard/hooks/` to your
  `assets/js/` directory and import them from there.

  ## Available Hooks

  ### DarkMode

  Manages dark mode toggle with localStorage persistence. Attach to the dark
  mode toggle button:

      <button id="dark-mode-toggle" phx-hook="DarkMode">...</button>

  ### KeyboardShortcuts

  Handles global keyboard navigation shortcuts. Attach to the main dashboard
  container with a `data-base-path` attribute:

      <div id="keyboard-shortcuts" phx-hook="KeyboardShortcuts" data-base-path="/pgflow">
        ...
      </div>

  Shortcuts:
  - `g o` - Go to Overview
  - `g w` - Go to Workers
  - `g f` - Go to Flows
  - `g r` - Go to Runs
  - `d` - Toggle dark mode
  - `?` or `K` - Open shortcuts modal
  - `Esc` - Close modal

  ### ShortcutsModal

  Manages the keyboard shortcuts help modal. Attach to the modal container:

      <div id="shortcuts-modal" phx-hook="ShortcutsModal" class="hidden">
        ...
      </div>

  ## Theme Initialization

  For the best user experience, add this script to your root layout's `<head>`
  to prevent flash of unstyled content:

      <script>
        (function() {
          const saved = localStorage.getItem('pgflow-theme');
          if (saved) {
            document.documentElement.setAttribute('data-theme', saved);
          } else if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
            document.documentElement.setAttribute('data-theme', 'dark');
          }
        })();
      </script>

  """

  @doc """
  Returns the path to the hooks directory.

  This can be used to copy hooks to your assets directory:

      Mix.install([])
      File.cp_r!(PgFlowDashboard.Hooks.hooks_path(), "assets/js/pgflow_hooks")

  """
  @spec hooks_path() :: String.t()
  def hooks_path do
    :pgflow
    |> :code.priv_dir()
    |> Path.join("static/pgflow_dashboard/hooks")
  end
end
