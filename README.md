# PgFlow

[![Hex.pm](https://img.shields.io/hexpm/v/pgflow.svg)](https://hex.pm/packages/pgflow)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/pgflow)
[![License](https://img.shields.io/hexpm/l/pgflow.svg)](https://github.com/agoodway/pgflow/blob/main/LICENSE)

> Workflows, background jobs and cron in Elixir and Postgres powered by PGMQ.

A native Elixir implementation of [pgflow](https://pgflow.dev) — a PostgreSQL-based workflow engine built on [pgmq](https://github.com/pgmq/pgmq). Define multi-step DAG workflows ("flows"), one-off background jobs ("jobs"), and scheduled cron jobs and flows — all backed by the same PostgreSQL queuing infrastructure with retries, visibility timeouts, and delivery guarantees. Built on OTP with supervised workers, adaptive backoff polling, and an optional LISTEN/NOTIFY strategy for low-latency task dispatch. Compatible with the TypeScript/Deno [pgflow](https://github.com/pgflow-dev/pgflow) project, sharing the same database schema and SQL functions.

## Why PgFlow?

- **No extra infrastructure** — Runs entirely in PostgreSQL using pgmq. No Redis, no external queue service.
- **Queryable state** — All workflow state lives in SQL tables. Debug with `SELECT * FROM pgflow.runs`.
- **Automatic retries** — Failed steps retry with exponential backoff. Only failed steps retry, not the whole workflow.
- **Parallel processing** — Steps run concurrently when dependencies allow. Fan-out with `map` for array processing.
- **Conditional steps** — Elixir supports pgflow 0.14 conditional steps (`if` / `if_not` / `when_unmet` / `when_exhausted`).
- **Cross-language** — Same flows can be processed by Elixir or Deno (Supabase) workers side-by-side.

## Further Reading

- [COMPARISON.md](docs/COMPARISON.md) — PgFlow vs Oban, Broadway, Temporal, Inngest, and others
- [ELIXIR_VS_SUPABASE.md](docs/ELIXIR_VS_SUPABASE.md) — Elixir vs Deno/TypeScript (Supabase) implementation
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — OTP supervision tree, worker model, and internals
- [UPSTREAM_COMPATIBILITY.md](docs/UPSTREAM_COMPATIBILITY.md) — Pinned upstream SHA, overlays, and schema contract
- [UPGRADING_UPSTREAM_2026_09.md](docs/UPGRADING_UPSTREAM_2026_09.md) — Upgrade to core V02 and helpers V05
- [UPGRADING_UPSTREAM_2026_10.md](docs/UPGRADING_UPSTREAM_2026_10.md) — Stop-and-drain upgrade to pgflow 0.17.2 (core V03, helpers V07)
- [LIVE_CLIENT.md](docs/LIVE_CLIENT.md) — LiveView integration for real-time flow/job tracking

## Prerequisites

- Elixir 1.18+
- PostgreSQL 17+ with:
  - **pgmq** — pgflow's queue backbone. The `mix pgflow.gen.pgmq_migration` task installs it via SQL (works on Neon, self-hosted, and any plain Postgres). Skip this step if your environment already ships pgmq (e.g. Supabase projects or managed services where pgmq is pre-enabled).
  - **pg_cron** (only for cron-scheduled flows/jobs) — requires two server-level settings before `CREATE EXTENSION pg_cron` will succeed:
    - `shared_preload_libraries = 'pg_cron'` (requires a Postgres restart)
    - `cron.database_name = '<your_app_db>'` — pg_cron's metadata lives in exactly one DB; defaults to `postgres`

    Supported on Neon, AWS RDS (PG 12.5+), Aurora (PG 12.6+), and Supabase — each with its own setup path (parameter groups, API calls, etc. — check your host's docs). If your host doesn't support pg_cron, generate extensions with `mix pgflow.gen.postgres_extensions_migration --no-cron` — cron-scheduled flows/jobs become unavailable but the rest of pgflow works.
  - Standard extensions: `citext`, `pg_trgm`, `pgcrypto`.
- An Ecto repository

The provided Docker setup (Postgres 17) includes all extensions pre-configured.

## Installation

Add `pgflow` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:pgflow, "~> 0.7.0"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

### Upgrading to 0.7

- Helpers V07 includes `pgflow.delete_run(uuid)`, which `PgFlow.Runs.delete/2`
  now calls, and a `prune_data_older_than` that locks runs before their tasks.
  Both remove lock cycles with claims and stalled-task recovery. A database
  that applied a pre-release helpers V07 lacks `delete_run`
  (`mix pgflow.check_schema` reports it): recreate it or re-apply those two
  definitions, as described in
  [UPGRADING_UPSTREAM_2026_10.md](docs/UPGRADING_UPSTREAM_2026_10.md).
- PgFlow has no per-run cancel or fail API. `PgFlow.Runs.delete/2` deletes run
  history and cannot interrupt a handler that is already running.
- `PgFlow.start_worker/2` now raises `ArgumentError` for any option other than
  `:repo`, `:max_concurrency`, and `:batch_size`, and for invalid values. Earlier
  releases ignored unknown options. It returns
  `{:error, {:conflicting_worker_options, pid}}` when a worker is already running
  with different configured limits.
- With `signal_strategy: :notify`, the first boot after the upgrade recreates
  each flow queue's pgmq notify trigger as `DEFERRABLE INITIALLY DEFERRED`, so a
  long producer transaction no longer blocks other producers on the same queue.
  Later boots that find the deferred trigger only reconcile the queue's
  throttle row, with no DDL. A trigger failure now fails the producer's COMMIT
  instead of its INSERT, and a transaction that sends to a queue and then drops
  or purges it must first run
  `SET CONSTRAINTS pgmq.trigger_notify_queue_insert_listeners IMMEDIATE`
  (`PgFlow.Queries.Pgmq.flush_notify_events/1`; `PgFlow.delete_flow/1` does
  this itself). Set `notify_trigger: :pgmq_default` to keep pgmq's immediate
  trigger, on every node, and `notify_lock_timeout_ms` (default `2000`) to bound
  how long a boot waits for the queue table lock. See the Notify section of
  [ARCHITECTURE.md](docs/ARCHITECTURE.md).
- `mix pgflow.check_schema` now prints warnings, without failing, for flow
  queue notify triggers that are not deferred (for example after
  `ALTER EXTENSION pgmq UPDATE`), that have no `pgmq.notify_insert_throttle`
  row, or that sit on partitioned queues pgmq cannot notify for. Pass
  `--notify-trigger pgmq_default` when the app uses `notify_trigger: :pgmq_default`.
- `PgFlow.Signal.Notify` restores missing `pgmq.notify_insert_throttle` rows
  every 30 seconds. pgmq keeps that table UNLOGGED, so crash recovery and Neon
  compute restarts empty it and silence notifications until a row is restored.
- New: `PgFlow.scale_worker/2` and `PgFlow.check_worker/2` for local runtime
  scaling, and the `worker_options` config map for per-flow or per-job
  `max_concurrency` and `batch_size`. Both calls take `:timeout` (default
  `5_000` ms, or `:infinity`); on expiry the caller exits with
  `{:timeout, {GenServer, :call, _}}`.

## Quick Start

### 1. Database Setup

**For development**, use the provided Docker Compose which builds a pre-configured Postgres image:

```bash
docker compose up -d
```

This builds a Postgres 17 image with pgmq, pg_cron, and the pgflow schema pre-loaded. Database available at `localhost:54323` (user: `postgres`, password: `postgres`, database: `pgflow_test`).

**Resetting the database:** The pgflow schema is loaded by the Docker init script on first container creation only. If you drop the database (e.g. `mix ecto.reset`), you must re-apply it:

```bash
# Destroy the Docker volume and start fresh
docker compose down -v && docker compose up -d
```

**For your application**, generate consumer migrations using the setup tasks. Each writes one wrapper migration into your app's `priv/repo/migrations/`:

```bash
# 1. Install required Postgres extensions (citext, pg_trgm, pgcrypto, pg_cron).
#    Pass `--no-cron` if pg_cron isn't available on your host.
mix pgflow.gen.postgres_extensions_migration

# 2. Install pgmq (unless your Postgres already provides it as an extension —
#    e.g. Supabase. On most hosts pgmq isn't native; use this task).
mix pgflow.gen.pgmq_migration

# 3. Install the pgflow schema + Elixir helper functions. Add `--dashboard`
#    to also install the LiveView dashboard schema. `--no-helpers` skips
#    the Elixir-binding SQL helpers (only useful if you're using a
#    different client).
mix pgflow.setup

# 4. Apply everything.
mix ecto.migrate
```

The generated `setup_pgflow.exs` migration calls
`PgFlow.Migration.up(version: 3)` and `PgFlow.HelpersMigration.up(version: 7)`.
The versions are pinned so a later library release cannot change a replayed
migration. A bare `up()` installs the newest bundled version, so pin wrappers
generated by older releases to the versions they originally installed (see the
[0.17.2 upgrade guide](docs/UPGRADING_UPSTREAM_2026_10.md)).
Ecto does not rerun an already recorded wrapper when the dependency changes.
When a release adds a core or helpers version, generate a new wrapper with
`mix pgflow.setup --upgrade`. pgflow 0.7.0 follows the
[0.17.2 upgrade procedure](docs/UPGRADING_UPSTREAM_2026_10.md). The previous
core V02 upgrade stays in the
[September procedure](docs/UPGRADING_UPSTREAM_2026_09.md).

### 2. Define a Flow

```elixir
defmodule MyApp.Flows.ProcessOrder do
  use PgFlow.Flow

  @flow queue: :process_order, max_attempts: 3, base_delay: 5, timeout: 60

  step :validate do
    fn input, _ctx ->
      %{order_id: input["order_id"], valid: true}
    end
  end

  step :charge_payment, depends_on: [:validate] do
    fn deps, _ctx ->
      %{charged: true, amount: deps["validate"]["amount"]}
    end
  end

  step :send_confirmation, depends_on: [:charge_payment] do
    fn deps, _ctx ->
      %{sent: true}
    end
  end
end
```

See `PgFlow.Flow` moduledocs for the full DSL reference (step options, map macro, handler input, error handling).

### 3. Compile the Flow to Database

Workers compile new definitions at startup and verify existing definitions before polling.
Generated definition migrations remain available for existing deployment workflows:

```bash
mix pgflow.gen.flow_migration MyApp.Flows.ProcessOrder
mix ecto.migrate
```

For a production shape change, drain and stop the affected workers first. Prefer a
new flow slug to preserve old run history. Explicitly replacing an existing
definition through a generated migration or runtime upsert can delete its history;
back up and follow the [upgrade guidance](docs/UPGRADING_UPSTREAM_2026_09.md).

### 4. Configure and Start

```elixir
# config/config.exs
config :my_app, MyApp.PgFlow,
  repo: MyApp.Repo,
  flows: [MyApp.Flows.ProcessOrder, MyApp.Flows.Import],
  jobs: [MyApp.Jobs.SendEmail],
  heartbeat_interval: 10_000,           # refresh persisted worker health (maximum: 20s)
  signal_strategy: :notify,             # use LISTEN/NOTIFY for low-latency (requires pgmq 1.8+)
  worker_options: %{
    MyApp.Flows.Import => [max_concurrency: 4, batch_size: 2]
  }
```

Per-worker `max_concurrency` and `batch_size` override the global defaults for specific flows or jobs. Keys must appear in `:flows` or `:jobs`; an entry applies to that module's worker whether `PgFlow.FlowStarter` or `PgFlow.start_worker/2` starts it. `PgFlow.start_worker/2` accepts the same options and takes precedence over `worker_options`; a module started only at runtime (not listed in `:flows` or `:jobs`) takes its overrides from that call.

All options have sensible defaults — only `repo` is required. See `PgFlow.Config` for the full list (concurrency, batch size, poll intervals, recovery, etc.).

Workers refresh their persisted heartbeat independently of polling or notification activity. Hosts can query readiness with `PgFlow.Workers.healthy?(MyApp.Repo, "process_order")`, which returns `{:ok, true | false}` or an error tuple when the database is unavailable.

```elixir
# lib/my_app/application.ex
def start(_type, _args) do
  children = [
    MyApp.Repo,
    {PgFlow, Application.fetch_env!(:my_app, MyApp.PgFlow)}
  ]

  opts = [strategy: :one_for_one, name: MyApp.Supervisor]
  Supervisor.start_link(children, opts)
end
```

### Scaling workers at runtime

`PgFlow.scale_worker/2` and `PgFlow.check_worker/2` adjust a running worker on **this node only** (similar to Oban's `scale_queue/2` / `check_queue/2`):

```elixir
{:ok, _} = PgFlow.start_worker(MyApp.Flows.Import)
{:ok, status} = PgFlow.scale_worker(MyApp.Flows.Import, max_concurrency: 6)
PgFlow.check_worker(MyApp.Flows.Import)
```

Contract:

- **Local only** — affects the worker process on this BEAM node, not remote nodes or TypeScript edge workers.
- **Runtime only** — limits live in memory; a crash or supervisor restart restores values from config / `worker_options` / `start_worker/2`, not the last scaled values.
- **Scale-down** — never interrupts active task handlers; running tasks finish, then new claims respect the lower limit.
- **Scale-up** — triggers an immediate poll so queued work can start without waiting for the next scheduled poll or notify fallback.
- **Timeout** — both accept `timeout:` (default `5_000` ms) for the call to the worker. When it elapses the caller exits; a timed-out scale may already have been applied.

### 5. Trigger a Flow

```elixir
# Async — returns immediately with run_id
{:ok, run_id} = PgFlow.start_flow(MyApp.Flows.ProcessOrder, %{"order_id" => 123})

# Sync — waits for completion (with optional timeout)
{:ok, run} = PgFlow.start_flow_sync(:process_order, %{"order_id" => 123}, timeout: 30_000)

# Check run status
{:ok, run} = PgFlow.get_run(run_id)
{:ok, run} = PgFlow.get_run_with_states(run_id)
```

## Background Jobs

PgFlow supports simple background jobs — one-off tasks like sending emails or processing webhooks. Jobs are single-step flows under the hood, reusing the same queuing infrastructure, retries, and dashboard visibility.

```elixir
defmodule MyApp.Jobs.SendEmail do
  use PgFlow.Job

  @job queue: :send_email, max_attempts: 5, base_delay: 10, timeout: 120

  perform :deliver do
    fn input, _ctx ->
      Mailer.send(input["to"], input["subject"], input["body"])
      %{sent: true}
    end
  end
end
```

The step name in `perform :deliver do` is optional — when omitted, it defaults to the `@job` queue/slug value.

See `PgFlow.Job` moduledocs for the full options reference.

```bash
# Compile to database
mix pgflow.gen.job_migration MyApp.Jobs.SendEmail
mix ecto.migrate
```

```elixir
# Enqueue a job
{:ok, run_id} = PgFlow.enqueue(MyApp.Jobs.SendEmail, %{"to" => "user@example.com", "subject" => "Hello"})
```

## Cron Scheduling

Both flows and jobs support cron scheduling via [pg_cron](https://github.com/citusdata/pg_cron). Add a `cron` option to run on a schedule:

```elixir
@flow queue: :daily_report, cron: [schedule: "0 9 * * *", input: %{"type" => "daily"}]
```

```elixir
@job queue: :cleanup, cron: [schedule: "@hourly"]
```

The cron schedule SQL is generated automatically when you run `mix pgflow.gen.flow_migration` or `mix pgflow.gen.job_migration` and migrate.

## Mix Tasks

### One-time setup

Run these once when adding pgflow to a project. Migrations are applied via `mix ecto.migrate`.

| Task                                             | Description                                                 |
|--------------------------------------------------|-------------------------------------------------------------|
| `mix pgflow.gen.postgres_extensions_migration`   | Migration: citext, pg_trgm, pgcrypto, pg_cron               |
| `mix pgflow.gen.pgmq_migration`                  | Migration: pgmq via SQL-only install                        |
| `mix pgflow.setup`                               | Wrapper migration: core schema + helpers                    |
| `mix pgflow.setup --upgrade`                     | New forward-only wrapper for core/helpers version bumps       |
| `mix pgflow.gen.helpers_migration`               | Migration: Elixir helpers standalone (setup bundles these)  |
| `mix pgflow.stamp`                               | Adopt an existing pgflow schema into EctoEvolver tracking   |

### Per-flow / per-job

Run once per flow or job module. Each generates a migration that compiles the flow/job definition into the database.

| Task                                             | Description                                                 |
|--------------------------------------------------|-------------------------------------------------------------|
| `mix pgflow.gen.flow_migration MyApp.Flow`       | Generate migration to compile flow to database              |
| `mix pgflow.gen.job_migration MyApp.Job`         | Generate migration to compile job to database               |

### Verification & test DB

| Task                                             | Description                                                 |
|--------------------------------------------------|-------------------------------------------------------------|
| `mix pgflow.check_schema`                        | Verify pgflow database schema compatibility                 |
| `mix pgflow.test.setup`                          | Set up test database                                        |
| `mix pgflow.test.reset`                          | Reset test database (teardown + setup)                      |
| `mix pgflow.test.teardown`                       | Tear down test database                                     |

## Dashboard

PgFlow includes an optional Phoenix LiveView dashboard for monitoring workflows, jobs, workers, and cron schedules in real-time.

See [DASHBOARD.md](docs/DASHBOARD.md) for installation instructions.

## LiveView Integration

`PgFlow.LiveClient` provides a LiveView-native client for tracking flow and job runs in real-time. It manages PubSub subscriptions and applies incremental updates to `%Run{}` structs in your socket assigns:

```elixir
defmodule MyAppWeb.OrderLive do
  use MyAppWeb, :live_view
  alias PgFlow.LiveClient

  def mount(_params, _session, socket) do
    {:ok, LiveClient.init(socket, pubsub: MyApp.PubSub)}
  end

  def handle_event("process", params, socket) do
    {:ok, socket} = LiveClient.start_flow(socket, :process_order, params)
    {:noreply, socket}
  end

  def handle_info({:pgflow, _, _} = msg, socket) do
    {:noreply, LiveClient.handle_info(msg, socket)}
  end
end
```

Requires the `:pubsub` option in your PgFlow config. See [LIVE_CLIENT.md](docs/LIVE_CLIENT.md) for the full API, multiple run tracking, and struct reference.

## Demo App

See [`demo/README.md`](demo/README.md) for a Phoenix LiveView application demonstrating PgFlow with real-time flow visualization.

## Telemetry

PgFlow emits `:telemetry` events across worker, poll, task, and run lifecycles for monitoring and metrics collection. See `PgFlow.Telemetry` moduledocs for event names, measurements, and metadata.

## Testing

```bash
# Start the dedicated integration database
docker compose -f test/support/db/compose.yaml up -d

# Run tests
PGFLOW_REQUIRE_DB=1 mix test
# Run separately: migration tests change the shared test schema.
PGFLOW_REQUIRE_DB=1 mix test --only migration
```

Without a database at `localhost:54323` the suite silently excludes every `:integration` test and still reports success. Set `PGFLOW_REQUIRE_DB=1` (recommended in CI) to make an unreachable database fail the run instead.

## Compatibility with PgFlow TypeScript/Deno

Compatibility targets upstream pgflow 0.17.2
(`baa9e43f5f9122133deacbc3e8ffc80e284b4f50`), reported by
`PgFlow.upstream_sha/0` and `PgFlow.compatibility_report/0`. Elixir and
TypeScript 0.17.x workers share one database in flow mode. TypeScript still
coerces falsy handler output (`false`, `0`, `""`) to null; Elixir preserves
those values. See [UPSTREAM_COMPATIBILITY.md](docs/UPSTREAM_COMPATIBILITY.md)
and the [October upgrade guide](docs/UPGRADING_UPSTREAM_2026_10.md).

Worker tuning and notifications are runtime-only and do not touch the shared
schema. `worker_options`, `PgFlow.start_worker/2`, and `PgFlow.scale_worker/2`
play the role of the TypeScript worker's `maxConcurrent` and `batchSize`;
upstream has no runtime scaling. Upstream workers never use LISTEN/NOTIFY.
With `signal_strategy: :notify`, PgFlow recreates pgmq's insert trigger on
each flow queue as `DEFERRABLE INITIALLY DEFERRED` so a long producer
transaction no longer blocks other producers (`notify_trigger: :pgmq_default`
keeps pgmq's trigger). TypeScript workers and producers sharing those queues
are unaffected. See [ELIXIR_VS_SUPABASE.md](docs/ELIXIR_VS_SUPABASE.md#queue-notifications).

## License

MIT
