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
- **Cross-language** — Same flows can be processed by Elixir or Deno (Supabase) workers side-by-side.

## Further Reading

- [COMPARISON.md](docs/COMPARISON.md) — PgFlow vs Oban, Broadway, Temporal, Inngest, and others
- [ELIXIR_VS_SUPABASE.md](docs/ELIXIR_VS_SUPABASE.md) — Elixir vs Deno/TypeScript (Supabase) implementation
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — OTP supervision tree, worker model, and internals
- [LIVE_CLIENT.md](docs/LIVE_CLIENT.md) — LiveView integration for real-time flow/job tracking

## Prerequisites

- Elixir 1.17+
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
    {:pgflow, "~> 0.2.1"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

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

The generated `setup_pgflow.exs` migration just calls `PgFlow.Migration.up/0`
and `PgFlow.HelpersMigration.up/0` — new pgflow releases bump the vendored
SQL, not your migration list.

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

Before workers can process a flow, it must be "compiled" into the database. This creates the flow record, PGMQ queue, and step definitions:

```bash
mix pgflow.gen.flow_migration MyApp.Flows.ProcessOrder
mix ecto.migrate
```

> **Note:** If you start a worker for a flow that hasn't been compiled, you'll get a helpful error message with the exact command to run.

### 4. Configure and Start

```elixir
# config/config.exs
config :my_app, MyApp.PgFlow,
  repo: MyApp.Repo,
  flows: [MyApp.Flows.ProcessOrder],
  signal_strategy: :notify              # use LISTEN/NOTIFY for low-latency (requires pgmq 1.8+)
```

All options have sensible defaults — only `repo` is required. See `PgFlow.Config` for the full list (concurrency, batch size, poll intervals, recovery, etc.).

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
# Start the database (same as Quick Start step 1)
docker compose up -d

# Run tests
mix test
```

Without a database at `localhost:54323` the suite silently excludes every `:integration` test and still reports success. Set `PGFLOW_REQUIRE_DB=1` (recommended in CI) to make an unreachable database fail the run instead.

## Compatibility with PgFlow TypeScript/Deno

This Elixir implementation is compatible with the TypeScript/Deno version — same PostgreSQL schema, same SQL functions, same PGMQ message format. Workers can run side-by-side. See [ELIXIR_VS_SUPABASE.md](docs/ELIXIR_VS_SUPABASE.md) for a detailed comparison and schema divergences.

## License

MIT
