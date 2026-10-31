# Upstream Compatibility

This release aligns Elixir PgFlow with upstream pgflow 0.17.2 at git SHA
`baa9e43f5f9122133deacbc3e8ffc80e284b4f50`. The shared database contract is
core V03 and helpers V07. An Elixir 0.7.0 worker and a TypeScript 0.17.x
worker can use one database in flow mode.

The September 2026 alignment (core V02, helpers V05/V06) is the previous
release. Its procedure stays in
[UPGRADING_UPSTREAM_2026_09.md](UPGRADING_UPSTREAM_2026_09.md). Moving a
database that is already on that release to 0.17.2 is
[UPGRADING_UPSTREAM_2026_10.md](UPGRADING_UPSTREAM_2026_10.md).

## Version reporting

| API | Meaning |
| --- | --- |
| `PgFlow.core_version/0` | Legacy semantic string (`"0.5.0"`). It does not track the package version. |
| `PgFlow.upstream_sha/0` | Pinned upstream git commit for the schema contract. |
| `PgFlow.compatibility_report/0` | Release `0.17.2`, that SHA, and bundled core/helpers integer floors. |
| `PgFlow.Migration.current_version/0` | Bundled core EctoEvolver version (currently `3`). |
| `PgFlow.HelpersMigration.current_version/0` | Bundled helpers EctoEvolver version (currently `7`). |

Run `mix pgflow.check_schema` before deploying workers. It verifies signatures,
queue constraints, helper record layout, and installed version floors.
`mix pgflow.audit_slugs` is the read-only slug check; the V03 migration repeats
it and aborts on any finding.

## Shared contract

Both Elixir and TypeScript 0.17.x workers must see:

- Five-argument claim: `pgflow.start_tasks(text, bigint[], uuid, text, text)`.
  Flow-mode callers pass `NULL` for the step slug. The three-argument and
  four-argument overloads are gone.
- Startup compilation: `pgflow.ensure_flow_compiled(text, jsonb, text, jsonb)`.
  Elixir passes queue mode `'flow'` and a null route map.
- Worker registration: `pgflow.register_worker(uuid, text, text, text)`.
  Elixir stores `elixir-<package version>` in `workers.pgflow_version`.
- Eight-field `step_task_record`:
  `flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input, attempts_count`.
- Task statuses: `queued`, `started`, `completed`, `failed`, `skipped`, `cancelled`.
- Flow mode: one queue per flow, `lower(flow_slug)`, at most 47 characters.
- `flows.queue_mode`. Elixir serves `'flow'` only.
- Ascending `idx_runs_started_at`.

A worker that expects a different queue mode or route map gets
`{:error, {:flow_routing_mismatch, message}}`. `message` is the upstream text,
unchanged.

`PgFlow.Config` raises `ArgumentError` at boot when two configured flows or jobs
share a PGMQ queue name (the lowercased slug).

## Documented Elixir overlays

These are intentional and recorded in the compatibility harness manifest:

| Overlay | Reason |
| --- | --- |
| `flows.flow_type` column + check | Distinguish jobs from multi-step flows in Elixir APIs. |
| `attempts_count` on `step_task_record` | Real handler attempt count; preserved through claim. |
| `step_progress` view | Elixir-only read model. Upstream 0.17.2 has no such view. |
| Helper RPC functions | `register_worker`, `flow_exists`, `get_flow_input`, `get_step_output`, `escalate_permanently_stalled`, queue-aware `recover_stalled_tasks`, filtered `prune_data_older_than/2`. |
| `realtime` schema shim | No-op `realtime.send/4` on plain Postgres; Elixir broadcasts via Phoenix.PubSub. Never replaces a real Supabase `realtime.send`. |
| Omitted `ensure_workers()` and `pgflow_telemetry` | They require Supabase `net.http_post`. Elixir registers workers via OTP. |
| `SET LOCAL lock_timeout` | Keeps the imported timeout within the wrapper transaction. |

## Behavior corrections (not copied from upstream)

| Area | Elixir behavior |
| --- | --- |
| JSON scalar preservation | Elixir preserves valid JSON scalars (`false`, `0`, `""`) through handlers. The pinned TypeScript 0.17.2 worker still coerces falsy output. Upstream main after 0.17.2 (#708, `f67145b0`) fixes that coercion and leaves the SQL schema unchanged (whitespace only), so the divergence will clear on the next pin. |
| Step mode | Refused at boot, before compilation, even when `is_local()` is true. TypeScript step-mode workers are a separate deployment. |
| `is_local()` | The database JWT GUC matching the Supabase local default enables destructive recompilation. An unset GUC does not; `MIX_ENV=dev` alone grants no permission to delete history. |
| External waits | Not in this release. No signal store or manual tasks. |
| Cron scheduling | Elixir-only deployment operation via `pg_cron`; not invoked on every worker heartbeat. |
| SQL-owned archival | Queue operations respect persisted `(queue_name, message_id)`; pruning uses task snapshots. |

## Startup-only upstream vs retained Elixir APIs

Upstream `ensure_flow_compiled/4` owns definition verification at worker start.
Elixir retains compatibility APIs (`FlowCompiler`, `JobCompiler`, generated flow/job
migrations) so existing consumer migrations keep working. Changing stored
timeout, max attempts, or base delay still needs an explicit database change;
verification preserves those columns.

## Installation routes

All paths converge on core V03 + helpers V07:

1. **Fresh install** — extensions → pgmq → `mix pgflow.setup` → `mix ecto.migrate`.
2. **Existing V02 + helpers ≤V06** — stop-and-drain upgrade (see the October guide).
3. **Core-only legacy** — core V03 is required first. Generate a new wrapper whose
   `up/0` calls the pinned core (`PgFlow.Migration.up(version: 3)`) and then
   helpers (`PgFlow.HelpersMigration.up(version: 7)`). Do not rerun a stamped setup.

### Consumer migration history (0.7.0 behavior change)

A bare `PgFlow.Migration.up()` tracks the newest bundled library version, and so
does a bare `PgFlow.HelpersMigration.up()`. An old setup migration that calls
them installs V03 and V07 when history is replayed from an empty database (CI,
new machines). Older flow-compile migrations with legacy slug literals then fail
the V03 slug rules.

- Pin existing wrappers to the versions they originally installed
  (`up(version: N)`).
- Fix slug literals in historical compile migrations.
- Newly generated wrappers (`mix pgflow.setup`, `--upgrade`) are pinned to
  `up(version: 3)` and `up(version: 7)`.
- Do not pin helpers below 7 on core 3. Core V03 requires helpers V07, and
  replaying helpers V05 alone re-creates the old four-argument `start_tasks`.
- 0.7.0 workers refuse to boot (`{:schema_incompatible, ...}`) until the V03/V07
  wrapper has run, so migrate before rolling workers.

Details are in [UPGRADING_UPSTREAM_2026_10.md](UPGRADING_UPSTREAM_2026_10.md).

## Cross-language evidence

The harness (`test/support/upstream/run.exs`) runs catalog, pgTAP, and
TypeScript profiles against the pinned SHA. The demo is the consumer
acceptance bed. JSON falsy parity with TypeScript 0.17.2 is a known divergence that upstream has fixed (see above).

The GitHub workflow runs on pull request and `workflow_dispatch`. A branch
push alone does not start it.

## Permissions

The migration role needs permission to install extensions and create/alter the
PgFlow schemas, functions, types, tables, and indexes. The runtime role needs
schema usage, function execution, and the DML used by workers. Startup also runs
`ensure_flow_compiled` and potentially `pgmq.create` under the application role;
grant their required queue-creation privileges or pre-provision definitions and
verify startup under the restricted role. Queue tables live under `pgmq.q_*`;
flow-mode workers pass the canonical lowercase queue name to `start_tasks/5`.
