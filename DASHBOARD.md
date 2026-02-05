# PgFlow Dashboard

An optional Phoenix LiveView dashboard for monitoring pgflow workflow execution. Provides real-time visibility into workflow runs, workers, and flow statistics.

## Features

- **Real-time monitoring** - Watch workflow runs as they execute
- **Run detail view** - Visualize step dependencies with interactive SVG graphs
- **Worker health** - Monitor worker processes and their task throughput
- **Flow statistics** - View 24-hour metrics including success rates and durations
- **Dark mode** - Automatic theme switching with system preference support
- **Keyboard navigation** - Vim-style shortcuts for power users

## Installation

### 1. Generate the Migration

Run the Mix task to generate the dashboard migration:

```bash
mix pgflow_dashboard.gen.migration
```

This creates a migration that installs the `pgflow_dashboard` PostgreSQL schema with read-only views and query functions.

Then run the migration:

```bash
mix ecto.migrate
```

### 2. Add to Supervision Tree

Add PgFlowDashboard to your application's supervision tree in `lib/my_app/application.ex`:

```elixir
def start(_type, _args) do
  children = [
    MyApp.Repo,
    MyAppWeb.Endpoint,
    # Add the dashboard supervisor
    PgFlowDashboard
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

### 3. Add Routes

In your router (`lib/my_app_web/router.ex`), import the dashboard router and mount it:

```elixir
defmodule MyAppWeb.Router do
  use MyAppWeb, :router

  import PgFlowDashboard.Router

  # Your existing pipelines...

  scope "/" do
    pipe_through [:browser]

    pgflow_dashboard "/pgflow",
      repo: MyApp.Repo,
      pubsub: MyApp.PubSub
  end
end
```

### 4. Install JavaScript Hooks

The dashboard uses LiveView hooks for dark mode and keyboard shortcuts. Add them to your `assets/js/app.js`:

```javascript
import { DarkMode, KeyboardShortcuts, ShortcutsModal } from "../../deps/pgflow/priv/static/pgflow_dashboard/hooks"

let liveSocket = new LiveSocket("/live", Socket, {
  hooks: { DarkMode, KeyboardShortcuts, ShortcutsModal, ...yourOtherHooks }
})
```

### 5. Visit the Dashboard

Start your server and navigate to `/pgflow` to see the dashboard.

## Authentication (Required for Production)

**The dashboard exposes sensitive workflow data and should be protected in production.**

The recommended approach uses `pipe_through` to protect the initial HTTP request (this is what Phoenix LiveDashboard uses):

```elixir
scope "/" do
  pipe_through [:browser, :require_authenticated_admin]

  pgflow_dashboard "/pgflow",
    repo: MyApp.Repo,
    pubsub: MyApp.PubSub
end
```

If you use `mix phx.gen.auth`, you likely already have authentication plugs you can reuse.

### Alternative: on_mount Hook

You can also use the `:on_mount` option for LiveView-specific authentication:

```elixir
pgflow_dashboard "/pgflow",
  repo: MyApp.Repo,
  pubsub: MyApp.PubSub,
  on_mount: [{MyAppWeb.AdminAuth, :ensure_admin}]
```

Example `on_mount` hook:

```elixir
defmodule MyAppWeb.AdminAuth do
  import Phoenix.LiveView

  def on_mount(:ensure_admin, _params, session, socket) do
    case session["current_user"] do
      %{role: "admin"} = user ->
        {:cont, assign(socket, :current_user, user)}

      _ ->
        {:halt, redirect(socket, to: "/login")}
    end
  end
end
```

For maximum security, you can combine both approaches.

## Configuration Options

| Option               | Type    | Default      | Description                                                       |
|----------------------|---------|--------------|-------------------------------------------------------------------|
| `repo`               | atom    | *required*   | Your Ecto repository module                                       |
| `pubsub`             | atom    | *required*   | Your Phoenix.PubSub module                                        |
| `refresh_interval`   | integer | `5_000`      | Polling interval in milliseconds                                  |
| `time_zone`          | string  | `"UTC"`      | Time zone for timestamps                                          |
| `default_time_range` | atom    | `:last_24h`  | Default filter (`:last_hour`, `:last_24h`, `:last_7d`, `:last_30d`) |
| `max_grid_runs`      | integer | `50`         | Maximum runs in history grid                                      |
| `cache_ttl`          | integer | `5_000`      | Cache TTL for expensive aggregations                              |

Example with all options:

```elixir
pgflow_dashboard "/pgflow",
  repo: MyApp.Repo,
  pubsub: MyApp.PubSub,
  refresh_interval: 10_000,
  time_zone: "America/New_York",
  default_time_range: :last_7d,
  on_mount: [{MyAppWeb.Auth, :ensure_admin}]
```

## Dashboard Pages

### Overview (`/pgflow`)
High-level metrics including active workers, running flows, completion rates, and recent runs.

### Runs (`/pgflow/runs`)
Filterable list of workflow runs with status, progress, and duration. Click any run for details.

### Run Detail (`/pgflow/runs/:id`)
Detailed view of a single run including:
- Interactive dependency graph with step status
- Step-by-step execution timeline
- Input/output data inspection

### Flows (`/pgflow/flows`)
List of all registered flows with 24-hour statistics.

### Flow Detail (`/pgflow/flows/:slug`)
Flow configuration and run history with GitHub-style activity grid.

### Workers (`/pgflow/workers`)
Worker process monitoring with health status (healthy, stale, dead) and task throughput metrics.

## Keyboard Shortcuts

Press `?` or `K` to see all shortcuts:

| Shortcut  | Action                           |
|-----------|----------------------------------|
| `g o`     | Go to Overview                   |
| `g w`     | Go to Workers                    |
| `g f`     | Go to Flows                      |
| `g r`     | Go to Runs                       |
| `d`       | Toggle dark mode                 |
| `j` / `k` | Next/previous step (run detail)  |
| `]` / `[` | Next/previous record             |
| `Esc`     | Close modal                      |

## Optional: Performance Indexes

For high-traffic dashboards, generate optional indexes:

```bash
mix pgflow_dashboard.gen.indexes
mix ecto.migrate
```

This adds indexes optimized for dashboard queries on runs, step states, and workers.

## Troubleshooting

### Dashboard shows no data

Ensure the migration has run:

```sql
SELECT * FROM pgflow_dashboard.get_overview_metrics();
```

### Real-time updates not working

Verify your PubSub is configured correctly and the dashboard supervisor is running.

### Hooks not working

Check that JavaScript hooks are properly imported and registered with your LiveSocket.
