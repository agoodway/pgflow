# Upgrading to pgflow 0.17.2 (October 2026)

This guide moves an existing PgFlow Elixir installation from core V02 and
helpers V06 (or earlier) to core V03 and helpers V07. That schema is the
pgflow 0.17.2 contract (`baa9e43f5f9122133deacbc3e8ffc80e284b4f50`), so an
Elixir 0.6.0 worker and a TypeScript 0.17.x worker can share one database in
flow mode.

**There is no expand/contract path.** The upgrade is one transaction. Stop
workers first.

## Who needs this

Anyone whose database is still on core V02 and helpers V06 or earlier, and who
wants that database to serve TypeScript 0.17.x workers together with the Elixir
OTP worker. A fresh `mix pgflow.setup` already installs V03 and V07; this
guide is the in-place upgrade.

## Before you start

Upgrade the application dependency to pgflow 0.6.0, then audit slugs before
generating the migration:

```bash
mix pgflow.audit_slugs
```

No findings means the database can take V03. The migration never renames a
slug. For each finding, rename the flow or step in Elixir code, redeploy that
definition, and run the audit again.

| Kind | Reason | Fix |
| --- | --- | --- |
| `flow` or `step` | `empty` | Give the flow or step a slug. |
| `flow` or `step` | `too_long` | Shorten it to at most 128 characters. |
| `flow` or `step` | `bad_chars` | Use only letters, digits, and `_`. |
| `flow` or `step` | `bad_start` | Start with a letter. |
| `flow` or `step` | `bad_end` | End with a letter or digit. |
| `flow` or `step` | `double_underscore` | Remove `__`. That sequence is reserved for queue names. |
| `flow` or `step` | `reserved` | Do not use the slug `run`. |
| `flow` | `queue_name_too_long` | Shorten the flow slug so `lower(flow_slug)` is at most 47 characters. That string is the PGMQ queue name. |
| `step_collision` | `case_insensitive_duplicate` | Rename one step. Names that differ only by case collide. |

Take a backup before the outage. `down/0` does not reverse this upgrade.

## Stop and drain

1. Stop every Elixir worker and every TypeScript worker.
2. Stop producers (`start_flow`, enqueue, cron, and TypeScript `startFlow`).
3. Wait until this returns 0:

```sql
SELECT count(*) FROM pgflow.step_tasks WHERE status IN ('queued', 'started');
```

Queued tasks can stay. They resume after the upgrade. Started tasks should
finish or be returned to the queue before you migrate, so a worker is not
holding a message across the schema change.

## Upgrade

Generate a new wrapper. Ecto will not rerun an older setup migration.

```bash
mix pgflow.setup --upgrade
mix ecto.migrate
```

The new file is `<timestamp>_upgrade_pgflow_core_v03_helpers_v07.exs`. Its
`up/0` calls `PgFlow.Migration.up()` and then `PgFlow.HelpersMigration.up()`,
which apply core V03 and helpers V07 inside the migration transaction. Core
V03 sets `lock_timeout` to 10 seconds and runs the slug pre-flight. Any audit
finding aborts the transaction, so a failure leaves the previous schema in
place. Add `--dashboard` when the LiveView dashboard schema should move in the
same transaction.

## Deploy

1. Deploy the Elixir release that requires core V03 and helpers V07.
2. Deploy TypeScript workers on 0.17.x. Those flows must use flow mode.
3. Resume maintenance, workers, and producers.

Confirm with `mix pgflow.check_schema`. `PgFlow.compatibility_report/0` reports
`upstream_release: "0.17.2"`, `bundled_core_version: 3`, and
`bundled_helpers_version: 7`.

## What changed

- Flow mode keeps one queue per flow, named `lower(flow_slug)`. Step mode uses
  private step queues and a route map. Elixir workers serve flow mode only.
- `pgflow.start_tasks` is five arguments:
  `start_tasks(text, bigint[], uuid, text, text)`. The last argument is the
  step slug and is null for a flow-mode claim.
- `pgflow.ensure_flow_compiled` is four arguments:
  `ensure_flow_compiled(text, jsonb, text, jsonb)`. Elixir passes `'flow'` and
  `NULL` for the route map.
- `pgflow.register_worker` is four arguments. Elixir stamps
  `pgflow_version` as `elixir-<package version>`.
- `idx_runs_started_at` is ascending, matching upstream.
- A queue-mode or route mismatch at startup is `{:error, {:flow_routing_mismatch, message}}`.
  The message is the upstream text.

## Not supported

- Step-mode flows. The Elixir worker refuses them at boot and does not compile
  them. A TypeScript step-mode worker is a separate deployment, not a shared
  queue with the Elixir worker.
- `pgflow_telemetry` and `ensure_workers`. They depend on Supabase `pg_net`
  and are not installed. Elixir registers workers from OTP.

## Rollback

The wrapper is forward-only. `down/0` raises and there is no down migration.
Restore the backup taken before the stop-and-drain step.
