# PgFlow Dashboard

A Phoenix LiveView dashboard for monitoring PgFlow workflows, jobs, and cron schedules.

## Installation

### 1. Generate the Migration

```bash
mix pgflow_dashboard.gen.migration
mix ecto.migrate
```

This installs the `pgflow_dashboard` PostgreSQL schema with read-only views and query functions.

### 2. Add to Supervision Tree

In `lib/my_app/application.ex`:

```elixir
children = [
  MyApp.Repo,
  MyAppWeb.Endpoint,
  PgFlowDashboard
]
```

### 3. Add Routes

In `lib/my_app_web/router.ex`:

```elixir
import PgFlowDashboard.Router

scope "/" do
  pipe_through [:browser]

  pgflow_dashboard "/pgflow",
    repo: MyApp.Repo,
    pubsub: MyApp.PubSub
end
```

### 4. Install JavaScript Hooks

In `assets/js/app.js`:

```javascript
import { DarkMode, KeyboardShortcuts, ShortcutsModal, MobileMenu } from "../../deps/pgflow/priv/static/pgflow_dashboard/hooks"

let liveSocket = new LiveSocket("/live", Socket, {
  hooks: { DarkMode, KeyboardShortcuts, ShortcutsModal, MobileMenu, ...yourOtherHooks }
})
```

### 5. Visit the Dashboard

Start your server and navigate to `/pgflow`.

## Authentication

**Protect the dashboard in production.** Use `pipe_through` to guard the routes:

```elixir
scope "/" do
  pipe_through [:browser, :require_authenticated_admin]

  pgflow_dashboard "/pgflow",
    repo: MyApp.Repo,
    pubsub: MyApp.PubSub
end
```

Or use the `:on_mount` option for LiveView-level auth:

```elixir
pgflow_dashboard "/pgflow",
  repo: MyApp.Repo,
  pubsub: MyApp.PubSub,
  on_mount: [{MyAppWeb.AdminAuth, :ensure_admin}]
```

## Configuration

| Option               | Type    | Default      | Description                                                          |
| -------------------- | ------- | ------------ | -------------------------------------------------------------------- |
| `repo`               | atom    | *required*   | Ecto repository module                                               |
| `pubsub`             | atom    | *required*   | Phoenix.PubSub module                                                |
| `refresh_interval`   | integer | `5_000`      | Auto-refresh interval in ms                                          |
| `time_zone`          | string  | `"UTC"`      | Time zone for timestamps                                             |
| `default_time_range` | atom    | `:last_24h`  | Default filter (`:last_hour`, `:last_24h`, `:last_7d`, `:last_30d`) |
| `max_grid_runs`      | integer | `50`         | Max runs shown in the activity grid                                  |
| `query_timeout`      | integer | `10_000`     | Database query timeout in ms                                         |
| `enable_pubsub`      | boolean | `true`       | Enable real-time PubSub updates                                      |
| `cache_ttl`          | integer | `5_000`      | Cache TTL for aggregation queries in ms                              |

## Pages

### Overview (`/pgflow`)
Active workers, running count, 24h completions/failures, average duration. Lists recent runs and worker status.

### Flows (`/pgflow/flows`)
All registered flow definitions with 24h statistics (runs, success rate, avg duration).

### Flow Detail (`/pgflow/flows/:slug`)
Flow configuration, run history, and GitHub-style activity grid.

### Jobs (`/pgflow/jobs`)
Background job definitions with 24h statistics. Card view for 12 or fewer jobs, paginated table for more.

### Job Detail (`/pgflow/jobs/:id`)
Job configuration and run history.

### Crons (`/pgflow/crons`)
Scheduled recurring jobs showing cron expression, human-readable schedule, next run time, and active/inactive status.

### Cron Detail (`/pgflow/crons/:id`)
Schedule details and run history for a cron.

### Runs (`/pgflow/runs`)
Filterable list of all workflow and job runs with status, progress, and duration.

### Run Detail (`/pgflow/runs/:id`)
Interactive SVG dependency graph, step execution timeline, and input/output data inspection.

### Workers (`/pgflow/workers`)
Worker processes with health status (healthy, stale, dead) and active task counts.

### Worker Detail (`/pgflow/workers/:id`)
Worker metadata and task throughput.

## Keyboard Shortcuts

Press `?` to see all shortcuts.

| Shortcut  | Action           |
| --------- | ---------------- |
| `g o`     | Go to Overview   |
| `g w`     | Go to Workers    |
| `g r`     | Go to Runs       |
| `g f`     | Go to Flows      |
| `g j`     | Go to Jobs       |
| `g c`     | Go to Crons      |
| `d`       | Toggle dark mode |
| `?`       | Show shortcuts   |
| `Esc`     | Close modal      |

## Performance Indexes

For high-traffic dashboards:

```bash
mix pgflow_dashboard.gen.indexes
mix ecto.migrate
```

## Troubleshooting

**No data showing** -- Verify the migration ran: `SELECT * FROM pgflow_dashboard.get_overview_metrics();`

**Real-time updates not working** -- Check that PubSub is configured and the `PgFlowDashboard` supervisor is running.

**Hooks not working** -- Verify all four hooks (`DarkMode`, `KeyboardShortcuts`, `ShortcutsModal`, `MobileMenu`) are registered with your LiveSocket.
