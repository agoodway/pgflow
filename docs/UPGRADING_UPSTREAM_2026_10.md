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
slug. `mix pgflow.audit_slugs` finds the host app's repo on its own; pass
`--repo MyApp.Repo` only when the app has more than one.

| Kind             | Reason                       | Fix                                                                                                       |
| ---------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------- |
| `flow`/`step`    | `empty`                      | Give the flow or step a slug.                                                                             |
| `flow`/`step`    | `too_long`                   | Shorten it to at most 128 characters.                                                                     |
| `flow`/`step`    | `bad_chars`                  | Use only letters, digits, and `_`.                                                                        |
| `flow`/`step`    | `bad_start`                  | Start with a letter.                                                                                      |
| `flow`/`step`    | `bad_end`                    | End with a letter or digit.                                                                               |
| `flow`/`step`    | `double_underscore`          | Remove `__`. That sequence is reserved for queue names.                                                   |
| `flow`/`step`    | `reserved`                   | Do not use the slug `run`.                                                                                |
| `flow`           | `queue_name_too_long`        | Shorten the flow slug so `lower(flow_slug)` is at most 47 characters. That string is the PGMQ queue name. |
| `step_collision` | `case_insensitive_duplicate` | Rename one step. Names that differ only by case collide.                                                  |

### Clearing a finding

Renaming a slug in Elixir code does not clear the finding. The old `flows` and
`steps` rows stay in the database, and the foreign keys have no
`ON UPDATE CASCADE`, so the rows cannot be renamed in place either. Remove the
old definition while the system is drained, on the old schema:

1. Export any run history you need to keep. Step 2 deletes it.
2. Delete the flow under its old slug:

   ```sql
   SELECT pgflow.delete_flow_and_data('<old_slug>');
   ```

   This deletes that flow's runs, step states, tasks, and its PGMQ queue. It
   cannot be undone except from the backup.
3. Rename the flow or step in Elixir code.
4. Run `mix pgflow.audit_slugs` again. Repeat until it reports no findings.
5. Run the upgrade (see below).
6. Deploy the renamed code. The worker compiles the definition under the new
   slug at boot.

Take a backup before the outage. `down/0` does not reverse this upgrade.

### Consumer migration history

Starting with 0.6.0, a bare `PgFlow.Migration.up()` installs the newest core
version bundled in the library, and a bare `PgFlow.HelpersMigration.up()`
installs the newest helpers version. An old `setup_pgflow` migration that calls
them therefore installs V03 and V07 when you replay history from an empty
database (CI, a new machine, a fresh review environment). Older flow-compile
migrations that still contain legacy slug literals then fail the V03 slug
rules.

This is a behavior change in 0.6.0. Before you ship it:

- Pin each existing wrapper to the versions it originally installed, for
  example `PgFlow.Migration.up(version: 1)` and
  `PgFlow.HelpersMigration.up(version: 1)`. Later wrappers should pin their own
  versions too.
- Fix legacy slug literals in historical compile migrations so they pass the
  slug rules.
- Newly generated wrappers (`mix pgflow.setup`, `mix pgflow.setup --upgrade`)
  are pinned: `up(version: 3)` for core and `up(version: 7)` for helpers.

Databases that already ran those migrations are not affected, because Ecto does
not rerun them.

## Stop and drain

Do these in order:

1. Stop producers: `start_flow`, enqueue calls, cron schedules, and TypeScript
   `startFlow`.
2. Stop recovery and pruning jobs (stalled-task recovery and
   `prune_data_older_than`).
3. Stop anything that writes definitions: Elixir flow and job compile
   migrations, Elixir worker boot (which compiles definitions), and TypeScript
   compile commands.
4. Let workers finish their active handlers, then stop them gracefully. Do not
   kill them mid-handler.
5. Wait until this returns 0:

```sql
SELECT count(*) FROM pgflow.step_tasks WHERE status = 'started';
```

Queued tasks are kept. They stay in the queue and resume after the upgrade, so
do not wait for `queued` to reach 0; with the workers stopped it never will.
Waiting for `started` to reach 0 means no worker is holding a message across
the schema change.

## Upgrade

Generate a new wrapper. Ecto will not rerun an older setup migration.

```bash
mix pgflow.setup --upgrade
mix ecto.migrate
```

The new file is `<timestamp>_upgrade_pgflow_core_v03_helpers_v07.exs`. Its
`up/0` calls `PgFlow.Migration.up(version: 3)` and then
`PgFlow.HelpersMigration.up(version: 7)`, which apply core V03 and helpers V07
inside the migration transaction. Core V03 sets `lock_timeout` to 10 seconds and
runs the slug pre-flight. Any audit finding aborts the transaction, so a failure
leaves the previous schema in place. Add `--dashboard` when the LiveView
dashboard schema should move in the same transaction.

Core V03 requires helpers V07. Do not pin helpers below 7 on core 3. Replaying
helpers V05 alone re-creates the old four-argument `start_tasks`, which core V03
removed.

**Outage cost.** V03 drops and rebuilds `idx_runs_started_at` (descending to
ascending, matching upstream) inside the migration transaction, under an
exclusive lock. The time grows with the size of `pgflow.runs`. Check
`SELECT count(*) FROM pgflow.runs;` and size the maintenance window to match.

## Deploy

Migrate first. 0.6.0 workers refuse to boot until the V03/V07 wrapper has run:
the boot schema check returns `{:schema_incompatible, ...}`. Do not roll workers
onto 0.6.0 before the migration.

1. Run the migration (above).
2. Deploy the Elixir release that requires core V03 and helpers V07.
3. Deploy TypeScript workers on 0.17.x. Those flows must use flow mode.
4. Resume recovery and pruning jobs, workers, and producers.

Confirm with `mix pgflow.check_schema`, which also finds the host app's repo
without `--repo`. `PgFlow.compatibility_report/0` reports
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
- `PgFlow.Config` raises `ArgumentError` at boot when two configured flows or
  jobs share a PGMQ queue name (the lowercased slug).
- A queue-mode or route mismatch at startup is `{:error, {:flow_routing_mismatch, message}}`.
  The message is the upstream text.
- Bare `up()` calls track the newest bundled version (see "Consumer migration
  history"). Generated wrappers pin explicit versions.

## Not supported

- Step-mode flows. The Elixir worker refuses them at boot and does not compile
  them. A TypeScript step-mode worker is a separate deployment, not a shared
  queue with the Elixir worker.
- `pgflow_telemetry` and `ensure_workers`. They depend on Supabase `pg_net`
  and are not installed. Elixir registers workers from OTP.

## Rollback

The wrapper is forward-only. `down/0` raises and there is no down migration.
Restore the backup taken before the stop-and-drain step.
