# PgFlow Dashboard

A Phoenix LiveView dashboard for monitoring PgFlow workflows, jobs, and cron schedules.

## Installation

### 1. Install PgFlow

The dashboard reads the existing PgFlow schema through PgFlow's typed core APIs. For a new PgFlow installation, generate and run the standard PgFlow migration:

```bash
mix pgflow.setup
mix ecto.migrate
```

No separate dashboard database migration is required. The historical `PgFlowDashboard.Migration` and its `pgflow_dashboard` views and functions remain available for compatibility with external SQL consumers. Existing installations may leave those objects in place; the core-backed LiveView dashboard does not require them to be removed or upgraded.

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

### 4. Add LiveFilter Dependency

The dashboard's Runs page uses [LiveFilter](https://github.com/agoodway/livefilter) for filtering and pagination. Since it's an optional dependency of pgflow, you must add it explicitly:

```elixir
# mix.exs
def deps do
  [
    {:pgflow, "~> 0.1.0"},
    {:livefilter, "~> 0.2.0"}
  ]
end
```

Then fetch:

```bash
mix deps.get
```

### 5. Configure Assets

**JavaScript hooks** — Add both PgFlow Dashboard and LiveFilter hooks to your LiveSocket in `assets/js/app.js`:

```javascript
import { CopyToClipboard, DarkMode, GraphNodeKeyboard, KeyboardShortcuts, ShortcutsModal, MobileMenu } from "pgflow/priv/static/pgflow_dashboard/hooks/index.js"
import { hooks as liveFilterHooks } from "livefilter/priv/static/live_filter.js"

let liveSocket = new LiveSocket("/live", Socket, {
  hooks: { ...liveFilterHooks, CopyToClipboard, DarkMode, GraphNodeKeyboard, KeyboardShortcuts, ShortcutsModal, MobileMenu, ...yourOtherHooks }
})
```

**esbuild** — LiveFilter's JS import requires `NODE_PATH` to resolve `deps/`. In `config/config.exs`:

```elixir
config :esbuild,
  version: "0.25.4",
  my_app: [
    args: ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js),
    cd: Path.expand("../assets", __DIR__),
    env: %{
      "NODE_PATH" => Path.expand("../deps", __DIR__)
    }
  ]
```

**Tailwind** — Add the dashboard, LiveFilter, and DaisyUI component paths so Tailwind generates their classes. In your CSS file (e.g. `assets/css/app.css`):

```css
@source "../../deps/pgflow/lib/pgflow_dashboard";
@source "../../deps/daisy_ui_components";
@source "../../deps/livefilter";
```

Or if using `tailwind.config.js`:

```javascript
module.exports = {
  content: [
    // ... existing paths ...
    "../deps/pgflow/lib/pgflow_dashboard/**/*.*ex",
    "../deps/daisy_ui_components/**/*.*ex",
    "../deps/livefilter/**/*.*ex",
  ],
}
```

### 6. Visit the Dashboard

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

**No data showing** -- Verify the core PgFlow migrations ran and that `PgFlow.Metrics.overview(MyApp.Repo)` succeeds in an IEx session.

**Real-time updates not working** -- Check that PubSub is configured and the `PgFlowDashboard` supervisor is running.

**Hooks not working** -- Verify the dashboard hooks (`CopyToClipboard`, `DarkMode`, `GraphNodeKeyboard`, `KeyboardShortcuts`, `ShortcutsModal`, `MobileMenu`) and `liveFilterHooks` are registered with your LiveSocket.

**Runs page filters not rendering** -- Ensure `livefilter` is in your deps, esbuild `NODE_PATH` includes `deps/`, and Tailwind scans `pgflow`, `daisy_ui_components`, and `livefilter` paths.
