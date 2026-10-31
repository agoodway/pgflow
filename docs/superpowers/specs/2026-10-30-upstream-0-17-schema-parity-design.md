# Upstream pgflow 0.17.2 Schema Parity — Design

**Status:** Approved direction (Chase, 2026-10-30). This spec covers design only. The step-by-step build is in [the implementation plan](../plans/2026-10-30-upstream-0-17-schema-parity-plan.md).

**Date:** 2026-10-30
**Repository:** `agoodway/pgflow` (Elixir), base `main` @ `3770b0d`
**Upstream target:** `pgflow-dev/pgflow` tag `pgflow@0.17.2` = `baa9e43f5f9122133deacbc3e8ffc80e284b4f50` (2026-10-02)
**Current pin:** `pgflow-dev/pgflow@94490709f79ebf366141dd925b047f0c1013e759` (PR #679, 2026-09-13), shipped as core V02 + helpers V05/V06 in pgflow-ex 0.4.0/0.5.0

## 0. Relationship to earlier plans

This release follows [2026-09-14-upstream-typescript-alignment.md](../plans/2026-09-14-upstream-typescript-alignment.md), which shipped core V02 + helpers V05 at `94490709`, and its demo companion [2026-09-14-demo-upstream-alignment.md](../plans/2026-09-14-demo-upstream-alignment.md).

The following decisions carry over unchanged:
- shared SQL semantics plus an overlay allowlist
- `flow_type` and `attempts_count`
- `is_local()` caution
- `FlowCompiler`/`JobCompiler`/`upsert_flow` kept
- falsy-JSON preservation
- no per-step queues served by Elixir
- no rolling upgrade

Superseded are only the pin, the claim and compile signatures, and the required versions. The new plan's "Relationship to the 2026-09-14 plans" section has the full mapping.

## 1. Problem

The Elixir package installs the pgflow schema through EctoEvolver: core versions V01–V02 plus Elixir helper versions V01–V06. Its pinned upstream SQL is an **unreleased** snapshot from PR #679. Upstream later shipped 0.17.0–0.17.2 with a different end state:

- per-flow `queue_mode`
- new slug rules
- a 5-argument `start_tasks`
- a 4-argument `ensure_flow_compiled`
- route derivation
- `workers.pgflow_version`

Two consequences:

1. An Elixir-installed database does not match the catalog that upstream pgflow 0.17.x expects.
2. TypeScript 0.17.x workers (`@pgflow/edge-worker`, `@pgflow/core`) **cannot share** a database with Elixir workers today. They call `ensure_flow_compiled(slug, shape, queueMode, routes)` and `start_tasks(... step_slug => ...)`, which do not exist in V02, and they insert `workers.pgflow_version`, which is not a column there.

Chase's goal: **the database/schema must comply with upstream 0.17.2**, so that Elixir and TypeScript 0.17.x workers can run in **flow mode** against one database. The Elixir code stays idiomatic OTP and is not reshaped to mirror the TypeScript worker.

## 2. Decision summary

| Topic | Decision |
|---|---|
| Upgrade style | **Stop-and-drain (downtime)**. No expand/contract, no dual schema, no rolling mixed-version upgrade. |
| Core | One new core version **V03**. Applied after V02, it yields a `pgflow` catalog identical to upstream 0.17.2, minus the documented Elixir overlays. |
| Helpers | One new helpers version **V07**. It re-applies the Elixir overlays (8-column claim record, `register_worker`) on top of V03. |
| Slugs | The upstream rules apply unchanged. The migration **fails atomically** on invalid legacy slugs and **never renames** them. |
| Pre-flight | New read-only `mix pgflow.audit_slugs`, plus compile-time slug validation in the `@flow`/`@job` DSL. |
| Ecto Evolver | Library **unchanged**. `mix pgflow.setup --upgrade` is adjusted (versioned wrapper name and upgrade checklist). |
| Out of scope | Elixir step-mode worker (possible future phase), the `pgflow_telemetry` schema, `ensure_workers` / Supabase edge-function plumbing. |

### 2.1 Why not expand/contract (Oban rationale, inlined)

The earlier research looked at how Oban upgrades its schema. Oban's numbered `Oban.Migrations.Postgres.VNN` steps are additive, so old and new nodes can overlap during a rolling deploy.

That works for Oban because Oban owns both sides of its contract. Its job table and its queries are versioned together, and the changes are almost always "add a column/index, keep the old path working."

pgflow 0.17 is not additive:

- It **drops** the 4-arg `start_tasks` and the 2-arg `ensure_flow_compiled`/`_create_flow_from_shape`.
- It tightens a CHECK constraint (`slug_is_valid`).
- It changes the PGMQ routing contract.

Upstream itself says "There is **no rolling mixed-version upgrade**. Old workers cannot claim tasks against the new schema; new workers cannot run against the old schema."

Keeping both shapes alive at once (an expand/contract bridge) would mean:

- carrying overloads that make calls ambiguous in Postgres (see §5.3)
- a second schema copy or shim functions upstream never had
- a catalog that is never identical to 0.17.2, which defeats the parity goal

We therefore borrow from Oban only the *discipline*:

- numbered, forward-only, transactional versions
- one migration wrapper that runs core then helpers
- an explicit version check at boot (`PgFlow.SchemaCheck`)

We do **not** borrow the overlap model. Chase rejected expand/contract for this release.

## 3. Gap analysis (inlined)

The rows below compare our pin (`94490709`) against `pgflow@0.17.2` (`baa9e43f`). Compare view: <https://github.com/pgflow-dev/pgflow/compare/94490709...baa9e43f>.

| # | Area | Upstream 0.17.2 | Elixir today (core V02 + helpers V06) | Resolution |
|---|---|---|---|---|
| 1 | Migration lineage | Unreleased `20260913093141_pgflow_persist_queue_identity.sql` was **deleted** and folded into `20260915074120_pgflow_private_step_queues.sql` ([#683](https://github.com/pgflow-dev/pgflow/pull/683)) | V02 = the deleted file | V03 is a **delta** from the V02 state. The upstream file cannot be replayed verbatim (statements already applied by V02 are omitted by rule). |
| 2 | `flows.queue_mode` | `text NOT NULL DEFAULT 'flow'` + `queue_mode_is_valid` CHECK (`flow`/`step`) | missing | V03 |
| 3 | Slug rules | `is_valid_slug`: ≤128, `^[a-zA-Z_][a-zA-Z0-9_]*$`, not `run`, must start with a letter, must end alphanumeric, no `__`, `search_path=''`; constraints re-added | old rule (allows `_x`, `x_`, `a__b`) | V03 (atomic failure), audit task, DSL validation |
| 4 | Claim | `start_tasks(flow_slug text, msg_ids bigint[], worker_id uuid, queue_name text, step_slug text DEFAULT NULL)`, plpgsql, `plan_cache_mode=force_custom_plan`; 4-arg dropped | 4-arg SQL function (helpers V05 8-column overlay) | V03 + helpers V07 overlay |
| 5 | Compile | `ensure_flow_compiled(flow_slug text, shape jsonb, queue_mode text DEFAULT 'flow', route_map jsonb DEFAULT NULL)` returns `{status, differences, mismatchKind}`; 2-arg dropped | 2-arg | V03; Elixir calls the 4-arg form explicitly |
| 6 | `_create_flow_from_shape` | 3-arg with `p_queue_mode`; 2-arg dropped | 2-arg | V03 |
| 7 | `idx_steps_normalized_slug` | unique `(flow_slug, lower(step_slug))` | missing (only flows normalized index) | V03 |
| 8 | Advisory locks | `pg_advisory_xact_lock(1, hashtext(lower(slug)))` | case-sensitive key | V03 (function bodies) |
| 9 | `add_step` | refuses to overwrite / step-mode guard | older body | V03 |
| 10 | Force-skip across queues | [#686](https://github.com/pgflow-dev/pgflow/pull/686) `20260919152659_pgflow_fix_force_skip_multi_queue.sql` (`_assert_step_queue_available`, `_cascade_force_skip_steps`) | missing | V03 |
| 11 | Routing functions | `_resolve_step_queue_name`, `_derive_queue_routes`, `_assert_step_queue_available`, `_listed_queue_name` (exists) | partial | V03 |
| 12 | Workers | `workers.pgflow_version text NULL`, `idx_workers_started_at`, `idx_workers_stopped_at` (pgflow-schema DDL inside telemetry migration [#684](https://github.com/pgflow-dev/pgflow/pull/684)) | missing | V03 (statement allowlist) |
| 13 | `idx_runs_started_at` | upstream `ON pgflow.runs (started_at)` | helpers V01 and `mix pgflow_dashboard.gen.indexes` create `(started_at DESC)` with the **same name** | V03 drops the DESC index and creates upstream's ASC one. A btree scans backward, so `ORDER BY started_at DESC` keeps using it. Recorded in the manifest. |
| 14 | Telemetry | `pgflow_telemetry` schema, pg_cron + pg_net jobs (#684, [#692](https://github.com/pgflow-dev/pgflow/pull/692) `20261001194538_pgflow_telemetry_endpoint_fix.sql`) | none | **Skip** (out of scope). Not part of the `pgflow` schema contract. |
| 15 | `ensure_workers` | edge-function HTTP re-invocation via `net.http_post` | omitted since V01 | **Skip** (existing removal rule) |
| 16 | Stalled-task escalation | upstream issue [#689](https://github.com/pgflow-dev/pgflow/issues/689) | helpers V06 `escalate_permanently_stalled` (ahead of upstream) | Keep as an Elixir overlay |
| 17 | Falsy JSON outputs | [#708](https://github.com/pgflow-dev/pgflow/pull/708) merged on main after 0.17.2; release pending ([#709](https://github.com/pgflow-dev/pgflow/pull/709)) | Elixir preserves `false`/`0`/`""` | 0.17.2 TS still coerces them; documented as a known divergence |
| 18 | Dirty-runs work | issues [#699](https://github.com/pgflow-dev/pgflow/issues/699), [#702](https://github.com/pgflow-dev/pgflow/issues/702) | — | Watch only; not in 0.17.2 |

Additional findings:

- **Helpers ordering hazard.** Several helper versions re-create `start_tasks`:
  - helpers V03 creates the 3-arg form
  - helpers V05 drops 3-arg and creates the 4-arg 8-column form
  - the core V03 `start_tasks` DROP must therefore be `IF EXISTS`, because the 3-arg form is already gone wherever helpers V05 ran

  On a **fresh install**, the chain core V01→V03 then helpers V01→V07 briefly re-creates the 4-arg form (helpers V05). Helpers V07 must drop it again. Both run in one Ecto transaction, so no other session ever sees the intermediate state.
- **8-column overlay.** Helpers V03 adds `attempts_count` to `pgflow.step_task_record`. The V02 bundle already EXECUTEs a 7- or 8-column `start_tasks` body depending on the record's attributes. V03 reuses that dispatch for the 5-arg body.
- **Interop today vs after.** TS ≥0.17 cannot share a V02 database. After V03 + V07, TS 0.17.2 and Elixir can share a database in **flow mode**. Step-mode flows created by TypeScript are visible to Elixir but never claimed or compiled by it (§6.4).

Upstream references:

- 0.17.1 news post (queue identity, step queues, telemetry): <https://github.com/pgflow-dev/pgflow/blob/main/pkgs/website/src/content/docs/news/pgflow-0-17-1-persistent-queues-and-telemetry.mdx>
- Update guide, "Queue identity and private step queues (0.17.0)": <https://github.com/pgflow-dev/pgflow/blob/main/pkgs/website/src/content/docs/deploy/update-pgflow.mdx>
- Migrations at the 0.17.2 tag: <https://github.com/pgflow-dev/pgflow/tree/baa9e43f5f9122133deacbc3e8ffc80e284b4f50/pkgs/core/supabase/migrations>
- PRs: [#679](https://github.com/pgflow-dev/pgflow/pull/679), [#683](https://github.com/pgflow-dev/pgflow/pull/683), [#684](https://github.com/pgflow-dev/pgflow/pull/684), [#686](https://github.com/pgflow-dev/pgflow/pull/686), [#688](https://github.com/pgflow-dev/pgflow/pull/688), [#692](https://github.com/pgflow-dev/pgflow/pull/692), [#695](https://github.com/pgflow-dev/pgflow/pull/695), [#708](https://github.com/pgflow-dev/pgflow/pull/708), [#709](https://github.com/pgflow-dev/pgflow/pull/709). Issues [#650](https://github.com/pgflow-dev/pgflow/issues/650), [#651](https://github.com/pgflow-dev/pgflow/issues/651).
- Ecto Evolver: <https://github.com/agoodway/ecto_evolver>
- PostgreSQL function overload resolution: <https://www.postgresql.org/docs/17/typeconv-func.html>. `CREATE FUNCTION` / `CREATE OR REPLACE` limits: <https://www.postgresql.org/docs/17/sql-createfunction.html>

## 4. Goals and non-goals

**Goals**

1. After `PgFlow.Migration.up/0` + `PgFlow.HelpersMigration.up/0`, the `pgflow` schema matches a pristine 0.17.2 replay. The upstream harness catalog suite proves it, using only the overlays listed in §5.5.
2. TS `@pgflow/edge-worker@0.17.2` and `@pgflow/core@0.17.2` can:
   - compile/verify flows
   - start flows
   - claim, complete, and fail tasks

   on an Elixir-installed database, concurrently with Elixir OTP workers on the **same flow-mode flow**.
3. Existing V02/V06 databases upgrade in one transaction or not at all. Invalid legacy data stops the migration with an actionable message naming the offending rows.
4. Elixir authors learn about invalid slugs at **compile time**, not at deploy time.

**Non-goals**

- An Elixir step-mode worker, `withStepQueues()` equivalent, or polling per-step queues (possible future phase).
- `pgflow_telemetry`, pg_net/pg_cron telemetry jobs, `ensure_workers`, Supabase edge-function plumbing.
- Changes to the `ecto_evolver` library.
- Renaming, re-casing, or auto-repairing user slugs.
- Matching upstream main past 0.17.2 (falsy-output #708 lands in 0.17.3; see §9).

## 5. Architecture

### 5.1 Core V03: a reproducible delta bundle

Core V03 is generated, not hand-written, by the existing `mix pgflow.sync_upstream` pipeline (`lib/pgflow/upstream/bundle.ex`). The pipeline is generalized to a per-version **spec**:

- **Pinned SHA:** `baa9e43f5f9122133deacbc3e8ffc80e284b4f50`.
- **Source files and SHA-256s:**
  1. `20260915074120_pgflow_private_step_queues.sql`, whole file, with rule-based omissions
  2. `20260919152659_pgflow_fix_force_skip_multi_queue.sql`, whole file
  3. `20260920093533_pgflow_telemetry.sql`, **allowlist only**: the four `pgflow.*` DDL statements at the top of the file (`idx_runs_started_at`, `workers.pgflow_version`, `idx_workers_started_at`, `idx_workers_stopped_at`). Everything in the `pgflow_telemetry` schema is excluded.
  4. `20261001194538_pgflow_telemetry_endpoint_fix.sql` is **not** listed; it only touches `pgflow_telemetry`.
- **Transformations.** Each one is recorded in `v03_manifest.json` with its reason. Each rule declares an exact expected match count (guards declare 0), and the build fails on any other count.

| Rule | Applies to | Why |
|---|---|---|
| `set_local_lock_timeout` | `SET lock_timeout = ...` | Existing V02 rule. Scopes it to the Ecto transaction. |
| `omit_applied_in_v02` | 11 statements: `idx_flows_normalized_slug`; `is_valid_queue_name`; `DROP INDEX idx_step_tasks_message_id`; and for both `step_tasks` and `steps`, the `queue_name` ADD COLUMN, the backfill UPDATE and the NOT NULL + `queue_name_is_valid` ALTER; plus `idx_step_tasks_queue_message` and `_listed_queue_name` | V02 already applied these from the deleted #679 file. Some are spelled differently in V02 (`text NULL` vs `text`, clause order, quoting), so the spec module records an explicit **pair table**: whitespace-normalized 0.17.2 statement → whitespace-normalized V02 statement. The build fails if either side of a pair is missing. The catalog suite proves the end state is equal. |
| `drop_if_exists` | `DROP FUNCTION pgflow.start_tasks(text, bigint[], uuid)` | Already dropped by helpers V05 on upgraded databases. |
| `replace_desc_runs_index` | before `CREATE INDEX "idx_runs_started_at"` | Emits `DROP INDEX IF EXISTS pgflow.idx_runs_started_at` so upstream's definition wins (gap row 13). |
| `step_task_record_dispatch` | the 5-arg `start_tasks` | Reuses the V02 dispatch: a DO block inspects `pgflow.step_task_record` and EXECUTEs the 7- or 8-column body. The two existing `add_attempts_count` anchors (the `RETURNING` list and the final `SELECT` list) match the 0.17.2 body byte-for-byte, so `replace_once/4` is reused unchanged. |
| `reject_net_http_post` | any statement (expected count **0**) | Guard carried over from V02. None of the three 0.17.2 sources defines `ensure_workers` or calls `net.http_post`, so the V02 `omit_ensure_workers` rule is not listed for V03. If a future source adds either, the build fails instead of silently importing Supabase-only plumbing. |
| `slug_preflight` (addition) | prepended | A `DO` block that runs the same checks as `mix pgflow.audit_slugs` and raises one `check_violation` listing every offending flow/step. It leaves no catalog object behind. |

- **Down migration:** forward-only, `RAISE EXCEPTION` (same shape as `v02_down.sql`).

Files:

- `priv/pgflow_core/sql/versions/v03/{v03_up.sql, v03_down.sql, v03_manifest.json}`
- `lib/pgflow/migrations/core/v03.ex`
- `PgFlow.Migration` lists V01–V03

`mix pgflow.sync_upstream --checkout PATH --sha SHA --version 03 --check` regenerates the SQL and diffs it byte-for-byte in CI. Published V01 and V02 are frozen (`@published_versions ["01", "02"]`).

### 5.2 Helpers V07: Elixir overlays on the 0.17.2 core

`priv/pgflow_helpers/sql/versions/v07/v07_up.sql`, in order:

1. **Prerequisite guard:** a `DO` block that raises unless `pgflow.flows.queue_mode` exists ("helpers V07 requires core V03; run PgFlow.Migration.up/0 first in the same migration").
2. `DROP FUNCTION IF EXISTS $SCHEMA$.start_tasks(text, bigint[], uuid, text)` and `DROP FUNCTION IF EXISTS $SCHEMA$.start_tasks(text, bigint[], uuid)`. This removes anything helpers V03/V05 re-created on a fresh chain.
3. `CREATE OR REPLACE FUNCTION $SCHEMA$.start_tasks(flow_slug text, msg_ids bigint[], worker_id uuid, queue_name text, step_slug text DEFAULT NULL)`: the upstream 0.17.2 body plus `attempts_count`. The body is generated from the bundle into a recorded fragment `priv/pgflow_helpers/sql/fragments/start_tasks_eight_column_v03.sql`, checksum-verified by `PgFlow.Upstream.Claim`.
4. `DROP FUNCTION IF EXISTS $SCHEMA$.register_worker(uuid, text, text)` then `CREATE FUNCTION $SCHEMA$.register_worker(p_worker_id uuid, p_queue_name text, p_function_name text, p_pgflow_version text)`. It writes `pgflow_version` on insert and on conflict.
5. `recover_stalled_tasks`, `prune_data_older_than(interval, text[])`, `escalate_permanently_stalled`, `step_progress`: unchanged. They already use the persisted `step_tasks.queue_name` (V05/V06), which also covers step-mode tasks. Tests prove this (§8).

Down migration: forward-only `RAISE`. Restoring the 4-arg claim on top of core V03 would give an incoherent catalog.

`PgFlow.HelpersMigration` lists V01–V07.

### 5.3 Why overloads are dropped, not kept

PostgreSQL resolves function calls by argument count and type. With both `start_tasks(text,bigint[],uuid,text)` and `start_tasks(text,bigint[],uuid,text,text DEFAULT NULL)` present, a 4-argument call matches both, and Postgres raises `function ... is not unique`.

`CREATE OR REPLACE` cannot add or remove a parameter or drop a default. It can only replace a function with the identical argument list.

The only coherent end state is therefore a single 5-arg claim and a single 4-arg compile. That is exactly upstream's state, so it is also the parity state. The same reasoning applies to `register_worker`: the 3-arg form is dropped before the 4-arg form is created.

### 5.4 Elixir runtime adaptation (minimal, OTP-idiomatic)

| Module | Change |
|---|---|
| `PgFlow.Queries.Flows.start_tasks/5` | Calls `pgflow.start_tasks($1::text, $2::bigint[], $3::uuid, $4::text, NULL::text)`. The explicit fifth argument makes the dependency on the 5-arg signature visible. Elixir never sets `step_slug`. |
| `PgFlow.Queries.Flows.ensure_flow_compiled/3` | Calls `pgflow.ensure_flow_compiled($1::text, $2::jsonb, 'flow'::text, NULL::jsonb)`. It parses `mismatchKind`: `"shape"` → `{:error, {:flow_shape_mismatch, diffs}}` (unchanged); `"routing"` → `{:error, {:flow_routing_mismatch, diffs}}`. |
| `PgFlow.Worker.Bootstrap` | Before compiling, reads `pgflow.flows.queue_mode` for the slug. If it is `'step'`, returns `{:error, {:unsupported_queue_mode, slug, "step"}}` and never calls `ensure_flow_compiled`. Reason: in `is_local()` environments upstream would *recompile* (delete and re-create) a mismatching flow, so an Elixir worker booting in dev could otherwise wipe a TS-owned step-mode flow. |
| `PgFlow.FlowStarter.classify_worker_start_error/1` | `:flow_routing_mismatch` and `:unsupported_queue_mode` are `:permanent`. |
| `PgFlow.Queries.Workers.register_worker/4` | Public arity unchanged. Passes `PgFlow.Version.worker_stamp/0` = `"elixir-" <> Application.spec(:pgflow, :vsn)` as the fourth SQL argument. |
| `PgFlow.SchemaCheck` | Requires core ≥ 3 and helpers ≥ 7. Claim signature `pgflow.start_tasks(text,bigint[],uuid,text,text)`. Both legacy claim forms absent. Compile signature `pgflow.ensure_flow_compiled(text,jsonb,text,jsonb)`, with the 2-arg form absent. New checks `flows.queue_mode` and `workers.pgflow_version`. `register_worker(uuid,text,text,text)`. Error text names V03/V07. |
| `PgFlow` | `@upstream_sha` = `baa9e43f…`, and `compatibility_report/0` adds `upstream_release: "0.17.2"`. Package version 0.6.0. |

No new processes, no supervision-tree changes. Polling, LISTEN/NOTIFY acceleration, and connection discipline are unchanged.

### 5.5 Proof of parity: the upstream harness

`test/support/upstream/` (the harness in `run.exs`, `harness.ex`, `manifest.json`, `worker.mjs`) is retargeted to `baa9e43f`:

- **Catalog suite.** It diffs the Elixir-installed fixture database against a pristine replay of upstream migrations ≤ 0.17.2. Two pristine-replay rules:
  - The telemetry migration is replayed in a new mode `pgflow_ddl_only`, which keeps only the statements targeting `pgflow.`. Disposable compat databases cannot load pg_cron/pg_net.
  - `20261001194538` is `skip`.

  Allowed overlays (already in `manifest.json`, kept):
  - `ensure_workers` missing
  - `flows.flow_type`
  - `step_task_record.attempts_count`
  - `pgflow_version` / `extensions_version` tracking views
  - helper functions and helper overrides (`start_tasks`, `recover_stalled_tasks`, `prune_data_older_than`, `register_worker`)

  Nothing else may differ.
- **pgTAP suite.** Adds the upstream 0.17.2 test files for slugs, `ensure_flow_compiled`, `queue_mode/*`, `_cascade_force_skip_steps/*` and `delete_flow_and_data/drops_queues_by_routes`. They run against the Elixir-installed database.
- **TypeScript suite.** Builds `dsl`, `core`, `edge-worker` from the 0.17.2 checkout and runs `worker.mjs` scenarios. New scenarios:
  - TS and Elixir workers draining one flow-mode flow together, exactly-once completion
  - a TS-compiled flow verified by the Elixir bootstrap and the reverse
  - `workers.pgflow_version` stamped by both
  - an Elixir boot that refuses a TS step-mode flow

### 5.6 Slug guardrails

`PgFlow.Slug` (new, pure) implements the upstream rule once:

- `valid?/1`
- `validate/1` → `:ok | {:error, reason}`, where reason is one of `:empty | :too_long | :bad_chars | :reserved | :bad_start | :bad_end | :double_underscore`
- `queue_name/1` (= `String.downcase/1`)
- `@max_slug_length 128`, `@max_queue_name_length 47`

Callers:

- **Compile time.** `PgFlow.DSL.Validation.validate_slug!/3` runs from `PgFlow.Flow.__before_compile__/1` and `PgFlow.Job.__before_compile__/1`. It rejects invalid flow/job slugs, invalid step slugs, flow slugs whose queue name exceeds 47 characters, and step slugs that collide case-insensitively within a flow. The error is a `CompileError` with file/line, the rule broken, and the upstream reference.
- **Start time.** `PgFlow.Config.validate!/1` rejects configured `:flows` and `:jobs` whose slugs collide case-insensitively. Flows are separate modules, so this cannot be detected at compile time.
- **Upgrade time.** `mix pgflow.audit_slugs [--repo R] [--prefix pgflow]` runs read-only (`SET TRANSACTION READ ONLY`) against the live database. It reports:
  - invalid flow slugs and invalid step slugs under the new rule
  - case-only duplicate step slugs within a flow (the new `idx_steps_normalized_slug`; flows are already covered by V02's `idx_flows_normalized_slug`)
  - `lower(flow_slug)` longer than 47 characters

  (Duplicate `(queue_name, message_id)` pairs need no check: V02 already enforces `idx_step_tasks_queue_message`.)

  It exits non-zero when anything is found. The V03 `slug_preflight` DO block runs the same SQL (shared from `PgFlow.Upgrade.SlugAudit.findings_sql/1` and wrapped by `preflight_sql/1`), so the mix task and the migration cannot disagree.

### 5.7 `mix pgflow.setup --upgrade`

Today it always writes `<ts>_upgrade_pgflow.exs` / module `UpgradePgflow`. An app that already generated one for 0.4.0 would get a **duplicate migration name**, and Ecto refuses to run it. The task now derives the name from the target versions:

- file `<ts>_upgrade_pgflow_core_v03_helpers_v07.exs`
- module `<Repo>.Migrations.UpgradePgflowCoreV03HelpersV07`

The moduledoc embeds the stop-and-drain checklist and the `mix pgflow.audit_slugs` pre-step. The body is still `PgFlow.Migration.up()` then `PgFlow.HelpersMigration.up()` (plus the dashboard if `--dashboard` was given) in one transaction. `down/0` is forward-only.

## 6. Upgrade procedure (stop-and-drain)

This is the operator-facing contract. It is published in `docs/UPGRADING_UPSTREAM_2026_10.md` and the GoodSkills skill.

1. **Before the window:**
   - Upgrade the `:pgflow` dependency to 0.6.0.
   - Run `mix pgflow.audit_slugs` against production (read-only) and fix every finding by hand. A step slug is part of a flow's identity, so fixing it means a new flow definition, not a rename in place.
   - Generate the wrapper with `mix pgflow.setup --upgrade`.
   - Back up the database.
2. **Pause producers.** Stop anything that calls `PgFlow.start_flow`/`enqueue`, cron jobs, and TS `startFlow` callers.
3. **Drain.** Let running handlers finish while the old schema is still in place. Queued tasks and PGMQ messages are kept; you do not need to wait for queues to empty.
4. **Stop every worker:**
   - Elixir nodes (`PgFlow` supervision tree)
   - TS edge/Node workers
   - recovery/prune jobs
   - anything that writes flow definitions
5. **Migrate.** Run `mix ecto.migrate` (one transaction: core V03, then helpers V07). If it fails, nothing changed. Fix the reported rows and retry.
6. **Deploy matching code.** pgflow-ex 0.6.0 and, if shared, `@pgflow/*@0.17.2` workers.
7. **Resume** in this order: maintenance, then workers, then producers. Restore any worker `enabled` states you recorded before the window.

There is no rollback migration. Rollback means restoring the step-1 backup together with the previous package versions.

### 6.4 Shared-database rules after the upgrade

- Flow-mode flows can be served by any mix of Elixir and TS 0.17.2 workers. The queue is `lower(flow_slug)`.
- Step-mode flows (TS `withStepQueues()`) may live in the same database. Elixir refuses to bootstrap them, and its worker never polls their per-step queues.
- Every TS worker must be 0.17.x. TS ≤0.16 cannot run against V03.

## 7. Error handling

| Situation | Behavior |
|---|---|
| Invalid legacy slug at migration | The V03 preflight raises `check_violation` listing each `flow_slug`/`step_slug` and the rule it breaks. The transaction is rolled back and the core version stays 2. |
| Helpers V07 without core V03 | The prerequisite guard raises with the fix instruction. |
| Elixir node booted against an old schema | `PgFlow.SchemaCheck` fails the boot with "requires core V03 / helpers V07 (pgflow 0.17.2 schema)". |
| TS 0.16 worker against V03 | Upstream behavior: its calls fail on missing signatures. Documented, not shimmed. |
| Elixir bootstrap finds a step-mode flow | `{:error, {:unsupported_queue_mode, slug, "step"}}`, classified permanent. |
| Routing-only mismatch | `{:error, {:flow_routing_mismatch, diffs}}`, classified permanent. |
| Down migration | Forward-only exception naming the backup restore path. |

## 8. Testing strategy

- **Pure unit tests:**
  - `PgFlow.Slug` rule table
  - DSL compile errors
  - Bundle transformation rules (each rule's match count, V02-identity checks)
  - `mix pgflow.setup --upgrade` content
- **Migration tests** (`@moduletag :migration`, run separately):
  - fresh V01→V03 core-only
  - fresh full chain core V03 + helpers V07
  - upgrade from the V02/V06 fixture with seeded data preserved and `queue_mode='flow'` backfilled
  - atomic failure on each new slug rule
  - forward-only downs
  - exact signature sets
  - `idx_runs_started_at` definition
- **Equivalence test:** `PgFlow.Slug.valid?/1` equals `pgflow.is_valid_slug/1` over a generated corpus after V03.
- **Integration:**
  - Elixir worker lifecycle on V03/V07
  - recovery and prune of step-mode tasks created via SQL
  - worker version stamp
  - bootstrap refusing step mode, including with `is_local()` true, where the flow must stay `'step'` afterwards
  - one TS 0.17.2 worker and one Elixir worker completing the same flow-mode runs on one database
- **Demo acceptance:** the demo upgrades in place from its committed V02/V06 history through a new pinned wrapper (`PgFlow.Migration.up(version: 3)` then `PgFlow.HelpersMigration.up(version: 7)`). Its upgrade test, `mix pgflow_demo.verify_scenarios` and `mix precommit` pass.
- **Harness:** the `--suite all` catalog/pgTAP/TypeScript run against `baa9e43f` in CI (`.github/workflows/upstream-compatibility.yml`).
- **Reproducibility:** `mix pgflow.sync_upstream --check --version 03` passes against the 0.17.2 checkout. V02 is frozen. Regenerating it would need a second checkout at `94490709`, so a checksum test pins `v02_up.sql` to its manifest `output_sha256` instead.

## 9. Open questions and assumptions

1. **0.17.2 vs 0.17.3.** We target the released 0.17.2 tag. #708 (falsy outputs) is merged on upstream main, and release PR #709 is open. If 0.17.3 ships before execution, re-pin with a follow-up V04 rather than editing V03 after publication.
2. **`idx_runs_started_at`.** The assumption is that replacing the DESC index with upstream's ASC index is acceptable for the dashboard. Postgres can scan a single-column btree backward. The dashboard index generator's `IF NOT EXISTS` then becomes a no-op.
3. **`register_worker` signature change.** It goes from 3 to 4 SQL arguments, and only Elixir calls it. The stamp format is `elixir-<vsn>`. Upstream telemetry, which we skip, expects semver; nothing else parses this column.
4. **Version bump to 0.6.0.** A new core version plus a breaking SQL contract is a minor bump under 0.x.
5. **Flow slug ≤ 47 characters** is enforced at DSL compile time. Upstream's actual limit is on the queue name, `lower(flow_slug)`, so it is the same constraint.
6. **Jobs** (`@job`) follow the same slug rules, because a job is a single-step flow with its own queue.
7. **pgTAP known-divergence allowance.** A listed upstream pgTAP file may be excluded only when it fails solely because plain Postgres lacks a Supabase feature, such as realtime or pg_net. Each exclusion records its reason in the manifest.
8. **Legacy `PgFlow.core_version/0`.** `@pgflow_core_version "0.5.0"` is documented as a legacy string that does not track the package, so it stays as is. Only `mix.exs` `@version` moves to 0.6.0.
9. **`mismatchKind` mapping.** `"routing"` maps to `{:error, {:flow_routing_mismatch, diffs}}`. The diff strings are upstream's (`Queue mode differs: …`, `Step routes differ: …`) and pass through unchanged.
