# Upstream Compatibility

This release aligns Elixir PgFlow with upstream pgflow at git SHA
`94490709f79ebf366141dd925b047f0c1013e759`. Upstream package manifests may
still read `0.16.0`; queue-identity work targets an unreleased `0.17.0`
changeset. **Do not claim npm `0.17.0` compatibility until upstream publishes
it.** Report compatibility against the pinned SHA instead.

## Version reporting

| API | Meaning |
| --- | --- |
| `PgFlow.core_version/0` | Legacy semantic string (`"0.5.0"`). Stable for existing callers. |
| `PgFlow.upstream_sha/0` | Pinned upstream git commit for SQL semantics. |
| `PgFlow.compatibility_report/0` | SHA plus bundled core/helpers integer floors. |
| `PgFlow.Migration.current_version/0` | Bundled core EctoEvolver version (currently `2`). |
| `PgFlow.HelpersMigration.current_version/0` | Bundled helpers EctoEvolver version (currently `5`). |

Run `mix pgflow.check_schema` before deploying workers. It verifies signatures,
queue constraints, helper record layout, and installed version floors.

## Shared contract

Both Elixir and pinned TypeScript workers must see:

- Four-argument claim: `pgflow.start_tasks(text,bigint[],uuid,text)` — the sole
  final overload; returns real `attempts_count` from `UPDATE ... RETURNING`.
- No three-argument claim overload.
- Startup compilation: `pgflow.ensure_flow_compiled(text,jsonb)`.
- Eight-field `step_task_record`:
  `flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input, attempts_count`.
- Task statuses: `queued`, `started`, `completed`, `failed`, `skipped`, `cancelled`.
- Queue identity: persisted `queue_name` on `steps` and `step_tasks` with
  `queue_name_is_valid` checks; message identity `(queue_name, message_id)`.

## Documented Elixir overlays

These are intentional and recorded in the compatibility harness manifest:

| Overlay | Reason |
| --- | --- |
| `flows.flow_type` column + check | Distinguish jobs from multi-step flows in Elixir APIs/dashboard. |
| `attempts_count` on `step_task_record` | Real handler attempt count; preserved through claim. |
| Helper RPC functions | `register_worker`, `flow_exists`, `get_flow_input`, `get_step_output`, queue-aware `recover_stalled_tasks`, filtered `prune_data_older_than/2`. |
| `realtime` schema shim | No-op `realtime.send/4` on plain Postgres; Elixir broadcasts via Phoenix.PubSub. Never replaces a real Supabase `realtime.send`. |
| Omitted `ensure_workers()` | Requires Supabase `net.http_post`; Elixir registers workers via OTP. |
| `SET LOCAL lock_timeout` | Keeps the imported timeout within the wrapper transaction. |
| Seven/eight-column claim dispatch | Generated core supports the upstream record and the helper record with `attempts_count`. |
| EctoEvolver tracking views | `pgflow_version`, `extensions_version` placeholder columns. |

## Behavior corrections (not copied from upstream)

| Area | Elixir behavior |
| --- | --- |
| JSON scalar preservation | Elixir preserves valid JSON scalars (`false`, `0`, `""`) through handlers. Pinned TypeScript at `@94490709` coerces falsy output via `output \|\| null` — documented as **unresolved** in the harness manifest. |
| `is_local()` | The database JWT GUC matching the Supabase local default enables destructive recompilation. An unset GUC does not; `MIX_ENV=dev` alone grants no permission to delete history. See the upgrade guide. |
| External waits / per-step queues | **Not in this release.** No signal store, manual tasks, or PR #6 integration. |
| Cron scheduling | Elixir-only deployment operation via `pg_cron`; not invoked on every worker heartbeat. |
| SQL-owned archival | Queue operations respect persisted `(queue_name, message_id)`; pruning uses task snapshots. |

## Startup-only upstream vs retained Elixir APIs

Upstream `ensure_flow_compiled/2` owns definition verification at worker start.
Elixir retains compatibility APIs (`FlowCompiler`, `JobCompiler`, generated flow/job
migrations) so existing consumer migrations keep working. Destructive runtime upsert
is deprecated in favor of startup compilation — see upgrade guide.

## Installation routes

All paths converge on core V02 + helpers V05:

1. **Fresh install** — extensions → pgmq → `mix pgflow.setup` → `mix ecto.migrate`.
2. **Existing V01 + helpers V04** — coordinated upgrade (see upgrade guide).
3. **Core-only legacy** — apply helpers via new wrapper; do not rerun stamped setup.

## Cross-language evidence

The harness (`test/support/upstream/run.exs`) records profile results against
the pinned SHA. The demo provides consumer acceptance. JSON falsy parity with
pinned TypeScript remains unresolved — do not claim universal JSON parity.

The GitHub workflow is intended verification and has not yet been demonstrated
to pass on GitHub. Local reports must distinguish passes, unresolved upstream
behavior, and explicitly skipped cases.

## Permissions

The migration role needs permission to install extensions and create/alter the
PgFlow schemas, functions, types, tables, and indexes. The runtime role needs
schema usage, function execution, and the DML used by workers. Startup also runs
`ensure_flow_compiled` and potentially `pgmq.create` under the application role;
grant their required queue-creation privileges or pre-provision definitions and
verify startup under the restricted role. Queue tables live under `pgmq.q_*`;
workers pass the canonical lowercase queue name to `start_tasks/4`.
