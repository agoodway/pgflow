# PgFlow

Workflows, background jobs and cron in Elixir and Postgres powered by PGMQ

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
- PostgreSQL with [pgmq](https://github.com/pgmq/pgmq) extension
- An Ecto repository
- Optional: [pg_cron](https://github.com/citusdata/pg_cron) for cron-scheduled flows and jobs

The provided Docker setup (Postgres 17) includes all extensions pre-configured.

## Installation

Add `pgflow` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:pgflow, "~> 0.1.0"}
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

This builds a Postgres 17 image with pgmq, pg_cron, and the pgflow schema pre-loaded. Database available at `localhost:54322` (user: `postgres`, password: `postgres`, database: `pgflow_test`).

**Resetting the database:** The pgflow schema is loaded by the Docker init script on first container creation only. If you drop the database (e.g. `mix ecto.reset`), you must re-apply it:

```bash
# Destroy the Docker volume and start fresh
docker compose down -v && docker compose up -d
```

**For production**, copy migrations to your project:

```bash
mix pgflow.copy_migrations
mix pgflow.gen.extensions_migration
mix ecto.migrate
```

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
mix pgflow.gen.flow MyApp.Flows.ProcessOrder
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

  perform do
    fn input, _ctx ->
      Mailer.send(input["to"], input["subject"], input["body"])
      %{sent: true}
    end
  end
end
```

See `PgFlow.Job` moduledocs for the full options reference.

```bash
# Compile to database
mix pgflow.gen.job MyApp.Jobs.SendEmail
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

The cron schedule SQL is generated automatically when you run `mix pgflow.gen.flow` or `mix pgflow.gen.job` and migrate.

## Mix Tasks

| Task                                    | Description                                       |
|-----------------------------------------|---------------------------------------------------|
| `mix pgflow.gen.flow MyApp.Flow`        | Generate migration to compile flow to database    |
| `mix pgflow.gen.job MyApp.Job`          | Generate migration to compile job to database     |
| `mix pgflow.gen.extensions_migration`   | Generate migration for PgFlow worker SQL functions|
| `mix pgflow.copy_migrations`            | Copy pgflow schema migrations to your project     |
| `mix pgflow.check_schema`               | Verify pgflow database schema compatibility       |
| `mix pgflow.sync_test_sql`              | Download latest pgflow SQL for testing            |
| `mix pgflow.test.setup`                 | Set up test database                              |
| `mix pgflow.test.reset`                 | Reset test database (teardown + setup)            |
| `mix pgflow.test.teardown`              | Tear down test database                           |

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

## Compatibility with PgFlow TypeScript/Deno

This Elixir implementation is compatible with the TypeScript/Deno version — same PostgreSQL schema, same SQL functions, same PGMQ message format. Workers can run side-by-side. See [ELIXIR_VS_SUPABASE.md](docs/ELIXIR_VS_SUPABASE.md) for a detailed comparison and schema divergences.

## License

MIT
