# Upgrading to Upstream Alignment (September 2026)

This guide covers moving an existing PgFlow Elixir installation from baseline
v0.3.4 (core V01, helpers V04) to core V02 + helpers V05 aligned with upstream
`94490709f79ebf366141dd925b047f0c1013e759`.

**There is no mixed-version rolling upgrade.** All producers, workers, recovery,
pruning, and definition writers must be quiesced for the schema transition.

## Operator procedure

Follow these steps in order:

1. **Identify and backup** the target database (logical dump + WAL/archive per your policy).
2. **Record worker enablement** and all running applications that enqueue or process pgflow work.
3. **Pause producers** — stop new flow/job starts and cron triggers.
4. **Drain handlers under the old schema** — let in-flight tasks complete; do **not** empty queues.
5. **Stop OTP applications** — process workers, HTTP re-invocation, and Elixir supervisors.
6. **Stop maintenance writers** — recovery, pruning, and definition mutation jobs.
7. **Apply core + helpers in one transaction** — generate and run a **new** upgrade wrapper (below).
8. **Verify** — `mix pgflow.check_schema`, spot-check signatures, queue backfill, and queue preservation.
9. **Deploy matching callers** — Elixir release and any TypeScript workers pinned to the same SHA.
10. **Restore maintenance** and **exact saved worker states** (enabled/disabled, start modes).
11. **Resume producers**.

### Failure and rollback semantics

- **Failed migration (transaction rolled back):** retry under the old schema after fixing the cause.
- **Successful migration:** old workers **must not** restart — they lack the four-argument claim and queue-aware helpers.

## Generate the upgrade wrapper

An already-applied setup migration will **not** rerun because Ecto records it once.
**Never** advise rerunning a stamped old wrapper as the upgrade mechanism.

```bash
mix pgflow.setup --upgrade --repo MyApp.Repo
mix ecto.migrate
```

This creates a new timestamped migration whose `up/0` runs:

```elixir
PgFlow.Migration.up()
PgFlow.HelpersMigration.up()
```

If you use PgFlowDashboard, also add `PgFlowDashboard.Migration.up()` to this new
wrapper. Dashboard V04 fixes worker identity, job type, and load counts for
mixed-case flow slugs; its earlier published migration files remain unchanged.

Both calls execute inside the same Ecto migration transaction; do not set
`@disable_ddl_transaction true` on the wrapper. `down/0` is
forward-only:

```elixir
raise "This PgFlow upstream upgrade is forward-only; restore the coordinated backup to revert"
```

Restore from the coordinated backup taken in step 1 to revert.

## Prerequisites

Before migrating, confirm:

- pgmq installed (extension or vendored SQL migration).
- `citext`, `pg_trgm`, `pgcrypto` registered; `pg_cron` registered if you use cron.
- Application dependency reports bundled core version 2 and helpers version 5
  through `PgFlow.compatibility_report/0`.
- No three-argument `start_tasks` callers remain in deployed code.

These pre-flight queries must return no rows:

```sql
SELECT lower(flow_slug), count(*) FROM pgflow.flows
GROUP BY 1 HAVING count(*) > 1;
SELECT flow_slug FROM pgflow.flows WHERE length(flow_slug) > 47;
SELECT lower(flow_slug), message_id, count(*) FROM pgflow.step_tasks
WHERE message_id IS NOT NULL GROUP BY 1, 2 HAVING count(*) > 1;
```

V02 rewrites every `step_tasks` row to populate queue identity, scans constraints,
and builds a non-concurrent unique index. The wrapper holds an ACCESS EXCLUSIVE
lock until its transaction ends. Assess table/index size, free disk, WAL capacity,
and long-running transactions before scheduling the upgrade. Consider applying
your retention policy with `pgflow.prune_data_older_than(...)` before the outage.
The rewrite creates dead tuples and can increase table and WAL size substantially;
plan post-upgrade vacuuming and sufficient free space. Measure on a restored copy
of your own database. The ten-second `lock_timeout` only bounds lock acquisition,
not execution; `SET LOCAL` remains in effect for the rest of the wrapper transaction.

## Post-upgrade verification

```bash
mix pgflow.check_schema --repo MyApp.Repo
```

Expected checks include:

- Core version ≥ 2, helpers version ≥ 5.
- `start_tasks(text,bigint[],uuid,text)` present; three-argument overload absent.
- `ensure_flow_compiled(text,jsonb)` present.
- `step_tasks.queue_name` and `steps.queue_name` NOT NULL with `queue_name_is_valid`.
- Eight-field `step_task_record` including `attempts_count`.
- Task statuses include `skipped` and `cancelled`.

Spot-check queued messages: persisted `(queue_name, message_id)` pairs should
survive the migration without queue draining.

```sql
SELECT to_regprocedure('pgflow.start_tasks(text,bigint[],uuid,text)') IS NOT NULL AS claim_ok,
       to_regprocedure('pgflow.start_tasks(text,bigint[],uuid)') IS NULL AS legacy_gone;
SELECT count(*) FROM pgflow.step_tasks WHERE queue_name IS NULL;
SELECT count(*) FROM pgflow.steps WHERE queue_name IS NULL;
SELECT queue_name, count(*) FROM pgflow.step_tasks
WHERE status = 'queued' GROUP BY 1;
```

Compare queue counts to your pre-upgrade snapshot; both null counts must be zero.

## Version skew and definition changes

| Symptom | Action |
| --- | --- |
| Old 0.3.4 worker repeatedly reports missing `start_tasks(text,bigint[],uuid)` | Stop it and deploy the matching release. Reservations expire, but work cannot proceed. |
| Worker exits with `{:bootstrap_failed, {:schema_incompatible, key}}` | Run `mix pgflow.check_schema` and apply the coordinated upgrade. The app can boot with no usable workers. |
| `:flow_shape_mismatch` | Correct the deployed definition or use a new slug. Startup preserves history unless destructive local mode is enabled. |

The schema check reports specific objects; resolve the cause before starting workers:

| Error family | Remedy |
| --- | --- |
| Schema, tables, core functions, core/helpers version missing or below the required floor | Confirm the selected database/repo and run a new coordinated upgrade wrapper. |
| pgmq missing | Install pgmq through the extension or generated pgmq migration before PgFlow. |
| Four-argument claim or startup compilation missing; obsolete three-argument claim present | Complete both core V02 and helpers V05 in the same wrapper. Do not mix releases. |
| `step_task_record` missing or layout mismatch | Install the matching helpers; field order and PostgreSQL types must match the runtime. |
| Task status constraint missing or missing values | Apply the matching core/helper migrations, then rerun the check. |
| `queue_name` missing, nullable, or missing `queue_name_is_valid` | Complete core V02 after resolving its pre-flight failures. |
| `failed to check` / `failed to inspect` / connection errors | Check database availability, selected repo, and role privileges; the embedded database error identifies the failing operation. |
| `:schema_incompatible` with `:ensure_flow_compiled`, `:track_worker_function`, `:start_tasks`, or `:start_tasks_legacy` | The worker refused startup before polling; inspect signatures with `mix pgflow.check_schema`. |

`is_local()` is controlled by the database GUC `app.settings.jwt_secret`, not by
`MIX_ENV`. It returns true when the GUC equals the Supabase local default
`super-secret-jwt-token-with-at-least-32-characters-long`. Supabase local instances
and this library's throwaway test database therefore permit destructive
recompilation. Never set this value on a database whose history must be preserved.

### Behavior changes

- `Runs.count/cancel/delete` operate on persisted task routes. Payload-keyed orphan
  messages without a task route are no longer swept by these operations.
- Non-JSON handler output fails the task and follows its configured retry policy;
  it is no longer completed with a synthetic `_raw` object.
- Editing DSL timeout, retry count, or base delay after compilation does not
  update stored options during verification. Apply an explicit, reviewed database
  migration for persisted options, or introduce a new slug. Shape replacement
  through destructive upsert can delete history.

## Deploy order

1. Migrate database (steps 7–8 above).
2. Deploy Elixir release compiled against the new library.
3. Deploy TypeScript/edge workers pinned to `@94490709` if used.
4. Re-enable cron and maintenance.
5. Resume producers.

## Known limitations after upgrade

- Deprecation replacements count toward the worker supervisor limit of ten
  restarts in sixty seconds, shared with crash restarts. Deprecate in batches
  below that limit with headroom for crashes; a broad sweep can terminate the
  supervisor and its other workers. Re-bootstrap without restarting is deferred.
- JSON falsy output parity with pinned TypeScript is unresolved — Elixir preserves scalars.
- External waits and per-step queues are follow-up work.
- Notify replacement behavior is unverified with the local pgmq 1.5.1 profile;
  replacements can degrade to fallback polling. Real notify acceptance needs pgmq
  with `enable_notify_insert` support.

## Demo reference

The demo app (`demo/`) exercises populated upgrade, feature catalogue, reconnect
behavior, and browser verification. See `demo/docs/SCENARIOS.md`.
