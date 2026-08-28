# PgFlow Core Operational Queries Design

## Goal

Make PgFlow core the sole owner of operational workflow reads and lifecycle
operations so consumers, the bundled Dashboard, and the demo do not depend on
Dashboard query modules or hand-written SQL.

## Decisions

- Core domain modules expose repository-explicit APIs. This lets the Dashboard
  use the repository supplied by its router configuration and lets tests target
  a sandboxed repository without global configuration.
- Table-backed results use typed `PgFlow.Schema.*` structs.
- Calculated results use dedicated typed structs rather than anonymous maps.
- `PgFlowDashboard.Queries.*` modules are removed immediately. There is no
  compatibility or delegation layer.
- Dashboard LiveViews and the demo call core modules directly.
- Core queries use Ecto for static `pgflow.*` tables. Parameterized SQL is
  reserved for PostgreSQL features Ecto cannot safely express, chiefly dynamic
  PGMQ live/archive queue tables.
- Existing versioned Dashboard SQL remains immutable migration history. Runtime
  code stops depending on it; this change does not add a create-then-drop
  migration solely to erase inert historical objects.
- No repository is committed, tagged, pushed, or published without separate
  user approval.

## Core Modules

### `PgFlow.Runs`

Owns run inspection and lifecycle operations:

- `get/2` and `get_with_states/2`
- `list/2` and `count/2` with flow, type, status, time, cursor, and JSON-input
  containment filters
- `list_step_states/2`
- `list_step_tasks/3` and `get_step_task/4`
- `adjacent/3`
- `history/3`
- `make_available/2`
- transactional, idempotent `delete/2`

`delete/2` locks the run, removes matching live and archived PGMQ messages,
then removes task, state, and run rows in foreign-key order. It validates the
stored flow slug before interpolating a queue table identifier.

### `PgFlow.Workers`

Owns persisted worker inspection and cleanup:

- `get/2`, `list/2`, and `count/2`
- `list_tasks/3`
- `adjacent/3`
- idempotent `delete/2`

Worker health and task-load calculations return `PgFlow.WorkerSummary`.

### `PgFlow.Definitions`

Owns stored definitions rather than compile-time registry definitions:

- `get_flow/2`, `list_flows/2`, and `count_flows/2`
- `get_step/3` and `list_steps/2`
- `get_job/2`, `list_jobs/2`, and `count_jobs/1`
- `get_cron/2`, `list_crons/2`, and `count_crons/1`

Flow/job statistics return `PgFlow.DefinitionSummary`; scheduled definitions
return `PgFlow.CronSummary`.

### `PgFlow.Metrics`

Owns the operational overview calculation and returns
`PgFlow.OverviewMetrics`. It contains no Dashboard presentation behavior.

## Typed Data

The existing schemas are aligned with the installed tables. In particular:

- `Run` and task/state JSON fields accept any valid JSON value, including list
  output from map steps.
- `StepState` includes `flow_slug` and every persisted lifecycle field.
- `StepTask` includes attempts, worker ownership, queue timestamps, and stalled
  recovery fields; it does not invent a non-existent `input` column.
- `Worker` reflects queue name, function name, heartbeat, and stop timestamps.
- `Flow` includes its persisted flow type and retry policy.

`PgFlow.Type.JSON` is an Ecto type whose cast/load/dump contract accepts every
Jason-compatible JSON value. This fixes `get_run_with_states/2` for list-valued
step output.

Calculated structs are deliberately separate from table schemas:

- `PgFlow.RunSummary`
- `PgFlow.WorkerSummary`
- `PgFlow.DefinitionSummary`
- `PgFlow.CronSummary`
- `PgFlow.RunHistoryCell`
- `PgFlow.OverviewMetrics`

## Dashboard and Demo

All six `PgFlowDashboard.Queries.*` modules are deleted. LiveViews alias the
corresponding core module and consume structs with field access. Cron schedule
humanization remains presentation code in the Dashboard. Activity-grid shaping
remains a component concern over typed `PgFlow.RunHistoryCell` values.

The demo continues to use the parent checkout in development and test. Its
tests and application helpers use the new core APIs instead of direct PgFlow
table queries where an API now exists.

## Inbox Adoption

Inbox temporarily uses `{:pgflow, path: "../../../os/pgflow"}` while this work
is under development. Its PgFlow test fixtures use core APIs for worker
existence/deletion, task inspection, delayed-run visibility, run cleanup,
state listing, and JSON-input run counts. App-specific discovery of which runs
belong to an Inbox entity remains in Inbox, followed by core lifecycle calls.

## Error Semantics

- Single-record reads return `{:ok, struct}` or `{:error, :not_found}`.
- Lists return `{:ok, [struct]}` so database failures are not silently changed
  into empty results.
- Counts return `{:ok, non_neg_integer()}`.
- Mutations return `:ok` or `{:error, reason}` and are idempotent for an absent
  run or worker.
- Invalid UUIDs return `{:error, :invalid_id}` without querying PostgreSQL.
- Invalid queue identifiers fail closed with `{:error, :invalid_flow_slug}`.

## Verification

Every new behavior follows red-green-refactor. Verification proceeds from
focused core tests to the full PgFlow suite and quality gate, then the demo
suite and quality gate, then focused Inbox PgFlow/email tests, `mix format`,
`mix quality`, and the complete Inbox Elixir suite. No green claim is made from
an earlier run after dependency or call-site changes.
