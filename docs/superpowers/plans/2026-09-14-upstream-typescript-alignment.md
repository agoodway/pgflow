# PgFlow Upstream TypeScript Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:subagent-driven-development instead only when the user authorizes delegation. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring PgFlow Elixir into compatibility with the merged upstream database and worker contracts at `pgflow-dev/pgflow@94490709f79ebf366141dd925b047f0c1013e759`, preserving documented Elixir additions and OTP behavior.

**Architecture:** Keep upstream SQL as the orchestration authority, imported as a reproducible, versioned delta. Reconcile Elixir helpers with that delta, then adapt the OTP worker, authoring/startup path, operational APIs, and dashboard. Verify both fresh installation and upgrades, and exercise real TypeScript callers against the Elixir-installed schema.

**Tech Stack:** Elixir/Ecto/Postgrex, OTP supervisors/GenServers/Tasks, EctoEvolver, PostgreSQL 17+, PGMQ, pg_cron, ExUnit, upstream pgTAP, pinned TypeScript packages and Node process workers.

**Companion plan:** [Demo acceptance and feature scenarios](2026-09-14-demo-upstream-alignment.md). The demo is a required consumer acceptance gate for this release. Its tasks D1–D4 cover migration, runnable scenarios, UI reconciliation, and end-to-end verification.

## Global constraints

- This document authorizes no implementation, database migration, commit, staging, push, release, or consumer-app changes by itself. The current assignment is writing the plan.
- At execution, preserve unrelated work and verify checkout/worktree identity. Research found branch `upstream-sync` at `9dbaaafdbf9ef5fcdfb2c10df8edea3577b07846`; do not assume that remains current.
- Preserve published migration files: core V01, helpers V01–V04, dashboard V01–V03. New behavior belongs in new versions.
- Retain Elixir's `flow_type`, real handler attempt count, jobs/cron API, telemetry, PubSub, optional LISTEN/NOTIFY, configurable recovery buffer, and repository ownership.
- Keep the package's existing Elixir version floor (`~> 1.17`). Do not raise it incidentally because the local toolchain is newer.
- Database state and transactions own correctness. Notifications accelerate polling only. Never hold a connection between poll cycles or throughout a handler.
- Use exact upstream SQL task identity `(run_id, step_slug, task_index)` and message identity `(queue_name, message_id)`; keep message IDs nullable on task rows and lossless at language boundaries.
- No public per-step/custom queues, manual tasks, new waiting state, signal store, or PR #6 integration in this release. Issues #651/#652/#660/#661 are future work.
- No mixed-version rolling upgrade promise. All producers, workers, recovery, pruning, and definition writers must be quiesced for the schema transition.
- Test in dedicated databases/containers. Do not reset or stop a shared development database. Tests that drop schemas run separately.
- Every implementation task uses a demonstrated failing regression, minimal implementation, passing focused tests, and review. Checkboxes indicate completed evidence, not dispatch or intent. No automatic commits.

## Evidence and scope

The research baseline is Elixir v0.3.4 at `9dbaaaf`, whose `priv/pgflow_core/sql/versions/v01/v01_manifest.json` pins upstream `5c132f3cd0c220cb4213fe12ef6ac1799f219a77`. Upstream main contains four additional migrations. Its package manifests still say 0.16.0; the merged queue change has a pending minor changeset and 0.17.0 documentation. Report compatibility with the pinned SHA until upstream actually releases that revision.

| Merged work | Required disposition | Tasks |
| --- | --- | --- |
| [#620](https://github.com/pgflow-dev/pgflow/pull/620), [#622](https://github.com/pgflow-dev/pgflow/pull/622): eager refill, bounded polling, connection ownership | Preserve existing OTP equivalents; test shutdown and capacity invariants | 7 |
| [#626](https://github.com/pgflow-dev/pgflow/pull/626): worker start mode | Import schema and register Elixir as `process` | 2, 6 |
| #627–#633, [#639](https://github.com/pgflow-dev/pgflow/pull/639): portable process runtime, startup/stop, replacement | Map lifecycle guarantees to OTP; no Node/Bun adapter port | 6, 7, 10 |
| [#649](https://github.com/pgflow-dev/pgflow/pull/649), [#663](https://github.com/pgflow-dev/pgflow/pull/663): skipped/cancelled tasks and late callbacks | Import exact terminalization/claim guards and retain regression coverage | 2, 4, 9 |
| [#664](https://github.com/pgflow-dev/pgflow/pull/664), [#667](https://github.com/pgflow-dev/pgflow/pull/667): effective timeouts, guaranteed visibility | Import and reconcile helper overrides | 2, 3, 4 |
| [#672](https://github.com/pgflow-dev/pgflow/pull/672): startup compilation | Adopt shared shape and mismatch contract; preserve legacy Elixir APIs as explicitly documented extensions | 5, 6 |
| [#679](https://github.com/pgflow-dev/pgflow/pull/679), [#650](https://github.com/pgflow-dev/pgflow/issues/650): queue identity | Apply throughout SQL, worker, maintenance, operational APIs | 2–4, 8–10 |
| [#675](https://github.com/pgflow-dev/pgflow/pull/675), [#680](https://github.com/pgflow-dev/pgflow/pull/680): test lifecycle/startup synchronization | Adopt isolation/readiness requirements in compatibility harness | 1, 10 |
| Documentation, CI, packaging, release merges, #618 docs API | Record as tooling/platform-specific; update Elixir docs where claims changed | 11 |

Read the final merged source, not the rejected implementation in open PR #677. In particular, do not introduce its new `claim_tasks` protocol, physical PGMQ inspection, or fatal batch classifier. PR #679's description records a portable-runtime CI failure despite local passes; merged status is not a substitute for our acceptance gates.

## Decisions carried into implementation

1. Compatibility means shared SQL semantics plus an explicit overlay allowlist. It does not mean reproducing JavaScript Promises, Supabase HTTP invocation, or JavaScript integer limitations in Elixir.
2. Preserve `flows.flow_type`: it is `text NOT NULL DEFAULT 'flow'` with `CHECK (flow_type IN ('flow','job'))`, not a PostgreSQL enum. Preserve dashboard indexes and the guarded plain-Postgres `realtime.send` shim. Never replace an existing real Supabase implementation with the shim.
3. Preserve the eighth `step_task_record.attempts_count` attribute and handler `ctx.attempt`. The new claim must return the incremented value from `UPDATE ... RETURNING`; do not calculate it from a stale candidate or PGMQ read count.
4. Add core V02 and helpers V05. The await-signals branch also called its unreleased helper V05; that branch is excluded and must be renumbered/reconciled if pursued later.
5. Startup uses upstream `ensure_flow_compiled(text,jsonb)`: missing definitions compile, matching definitions verify, production mismatch fails, and local mismatch follows upstream `is_local()`. Plain PostgreSQL normally returns false from that Supabase-specific detector; do not secretly equate `MIX_ENV=dev` with permission to delete run history.
6. Keep already-generated flow/job migrations executable. Do not delete `FlowCompiler`, `JobCompiler`, or public `upsert_flow` in this alignment release. Deprecate destructive runtime upsert and manual definition generation in documentation, with explicit replacement guidance. Removing them needs separate consumer migration scope. Cron-only scheduling remains an Elixir deployment operation and must not run on every worker heartbeat/start.
7. Preserve valid JSON values, including scalar and false/null values, through Elixir. A confirmed upstream client defect is recorded with a regression and limitation, not copied into Elixir to claim parity.

## File and dependency map

| Boundary | Files and responsibility |
| --- | --- |
| Import provenance | `lib/mix/tasks/pgflow.sync_upstream.ex`, `lib/pgflow/upstream/bundle.ex`, `priv/pgflow_core/sql/versions/v02/`: explicit local checkout, pinned manifest, SQL transformation |
| Database versions | `lib/pgflow/migrations/core/v02.ex`, `lib/pgflow/migration.ex`, `lib/pgflow/migrations/versions/v05.ex`, `lib/pgflow/helpers_migration.ex`: version registration and upgrade |
| Claim/recovery overlays | `priv/pgflow_helpers/sql/versions/v05/`: current four-argument claim, attempt count, queue-aware recovery/pruning |
| Shape/startup | `lib/pgflow/flow/shape.ex`, `lib/pgflow/worker/bootstrap.ex`, `lib/pgflow/queries/flows.ex`: upstream shape and pre-registration validation |
| OTP execution | `lib/pgflow/worker/server.ex`, `lib/pgflow/worker/lifecycle.ex`, `lib/pgflow/worker_supervisor.ex`, `lib/pgflow/flow_starter.ex`: drain/restart/readiness |
| Operational surface | `lib/pgflow/runs.ex`, `lib/pgflow/workers.ex`, `lib/pgflow/schema/{step,step_task}.ex`: persisted routes and statuses |
| Compatibility evidence | `test/pgflow/upstream/`, `test/support/upstream/`, `.github/workflows/upstream-compatibility.yml`: isolated SQL/TS matrix |

Execution order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11. These are review boundaries inside one release, not independently deployable releases. Tasks 2 and 3 must be integrated before claiming the default fresh install works. Finish the complete migration matrix before deploying any runtime change.

Execute demo D1 after Task 6, D2–D3 after Task 9, and D4 after Task 10. Task 11 cannot pass until demo D4 passes. The companion plan is separated by application ownership, not deferred to another release.

### Task 1: Create isolated compatibility fixtures and assert the baseline

**Files:** Modify `config/test.exs`, `test/test_helper.exs`; create `test/support/upstream/fixture.ex`, `test/support/upstream/manifest.json`, `test/pgflow/upstream/baseline_test.exs`.

**Interfaces:** `PgFlow.Test.UpstreamFixture` owns temporary test databases and installs explicit core/helper versions using Ecto.Migrator wrappers. It accepts a dedicated administrative URL and refuses the normal application database. `PGFLOW_TEST_PORT` and `PGFLOW_TEST_DATABASE` select the ordinary test connection consistently, including readiness checks and psql helper loading.

- [x] Add failing tests for the expected post-upgrade catalog: required queue columns, six task statuses, four-argument claim present, three-argument claim absent, two-argument compilation present, and the eight-field Elixir record. Include executable catalog assertions such as:

```sql
SELECT to_regprocedure('pgflow.start_tasks(text,bigint[],uuid,text)') IS NOT NULL;
SELECT to_regprocedure('pgflow.start_tasks(text,bigint[],uuid)') IS NULL;
SELECT to_regprocedure('pgflow.ensure_flow_compiled(text,jsonb)') IS NOT NULL;
SELECT attname FROM pg_attribute
WHERE attrelid = (SELECT typrelid FROM pg_type
  WHERE oid = 'pgflow.step_task_record'::regtype)
  AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
```

- [x] Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/upstream/baseline_test.exs` against the dedicated fixture. Expect missing queue/claim assertions to fail on v0.3.4, not a connection failure.
- [x] Parameterize every hardcoded test connection together:

```elixir
port = String.to_integer(System.get_env("PGFLOW_TEST_PORT", "54323"))
database = System.get_env("PGFLOW_TEST_DATABASE", "pgflow_test")
```

  Keep `PGFLOW_REQUIRE_DB=1` fail-closed and verify installed **core and helpers** versions. Fixture setup records baseline versions, seeds queued/started/completed/failed runs, jobs, map tasks, a mixed-case flow, and a task with null message ID. Use unique fixture names; cleanup only fixture-owned databases.
- [x] Run fixture lifecycle tests. Expect setup/teardown to leave unrelated databases untouched. Keep post-upgrade assertions failing until Tasks 2–3 land; do not mark those assertions passed.

### Task 2: Import the four upstream deltas with deterministic provenance

**Files:** Create `lib/mix/tasks/pgflow.sync_upstream.ex`, `lib/pgflow/upstream/bundle.ex`, `lib/pgflow/migrations/core/v02.ex`, `priv/pgflow_core/sql/versions/v02/{v02_up.sql,v02_down.sql,v02_manifest.json}`, `test/pgflow/upstream/bundle_test.exs`, `test/pgflow/upstream/core_upgrade_test.exs`; modify `lib/pgflow/migration.ex`. Use `lib/pgflow/sql/splitter.ex` for statement splitting; patch it only if a source fixture demonstrates a parsing failure.

**Interfaces:** `PgFlow.Upstream.Bundle.build(checkout, sha)` returns `{:ok, %{sql: sql, manifest: manifest}}` or a descriptive error. `mix pgflow.sync_upstream --checkout PATH --sha SHA --version 02 [--check]` generates an unpublished delta or checks committed output without writing. It rejects a checkout whose HEAD differs from SHA and refuses rewriting published versions.

- [x] Add deterministic import tests and a baseline upgrade regression. Assert that V01 bytes stay unchanged and that input source hashes, output hashes, ordered filenames, removals, and overlay transformations appear in the new manifest. Run `mix test test/pgflow/upstream/bundle_test.exs`; expect missing importer failure.
- [x] Import exactly these files, in this order, from the pin:

```text
20260607175525_pgflow_worker_start_mode.sql
20260904095427_pgflow_task_lifecycle_hardening.sql
20260907082520_pgflow_remove_legacy_flow_compilation.sql
20260913093141_pgflow_persist_queue_identity.sql
```

  Preserve original SQL except statement delimiters and recorded portability/overlay changes. Omit the `ensure_workers()` body requiring `net.http_post`; retain `worker_functions.start_mode` and `track_worker_function(text,text)`. Use transaction-local lock timeout rather than leaking upstream `SET lock_timeout` into a pooled connection. Reject unrecognized infrastructure dependencies instead of silently dropping arbitrary functions.

```elixir
defmodule PgFlow.Migrations.Core.V02 do
  @moduledoc "Upstream core delta through 94490709; see the bundled manifest."
  use EctoEvolver.Version,
    otp_app: :pgflow,
    version: "02",
    sql_path: "pgflow_core/sql/versions"
end
```

- [x] Generate each changed `start_tasks` installation in two reviewed variants: seven-column upstream and eight-column Elixir. Select at migration time by inspecting the existing composite type. For the eight-column **new** implementation, add `step_tasks.attempts_count` to the guarded update's `RETURNING` and `st.attempts_count` to the final projection. Reject any other attribute layout. Do not drop/recreate the shared type or use `CASCADE` to bypass dependencies.

```sql
-- Shape dispatch performed by the generated installation block:
-- seven expected attributes -> install exact upstream body;
-- same seven plus attempts_count integer -> install recorded additive variant;
-- anything else -> RAISE EXCEPTION before changing functions.
-- In the eight-column variant's tasks UPDATE:
RETURNING step_tasks.flow_slug, step_tasks.run_id, step_tasks.step_slug,
          step_tasks.task_index, step_tasks.message_id, step_tasks.attempts_count
-- In the final projection, append:
-- st.attempts_count
```

  This selection is needed for the lifecycle-hardening three-argument definition and the later four-argument definition, not just the final function. Fresh databases initially have seven fields; existing helpers V03/V04 databases have eight.
- [x] Register V02 after V01. Make V02 down fail explicitly with an actionable forward-only error: new statuses and queue identities cannot safely be discarded automatically. No destructive downgrade masquerading as rollback.
- [x] Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/upstream/core_upgrade_test.exs --only migration`. Expect successful core-only fresh install and upgrade from core V01 with helpers V04 already present; queue backfill preserves tasks/messages and existing `flow_type` data. Verify conflicting normalized slugs and duplicate queue/message pairs roll back the entire transaction, including version tracking.

### Task 3: Reconcile the helper chain and final installation order

**Files:** Create `lib/pgflow/migrations/versions/v05.ex`, `priv/pgflow_helpers/sql/versions/v05/{v05_up.sql,v05_down.sql}`, `test/pgflow/upstream/helpers_upgrade_test.exs`; modify `lib/pgflow/helpers_migration.ex`, `lib/mix/tasks/pgflow.setup.ex`, `test/test_helper.exs`.

**Interfaces:** Core V02 plus helpers V05 exposes exactly one claim signature, `start_tasks(text,bigint[],uuid,text)`, returning eight fields. `recover_stalled_tasks(double precision)` continues returning `(recovered_count bigint, vt_batches bigint)`. The existing filtered prune API keeps its return fields and arguments.

- [x] Add regressions for all three routes: empty DB → latest core/helpers; V01+V04 → latest; latest core without helpers → full helper chain. Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/upstream/helpers_upgrade_test.exs --only migration`; expect failures from obsolete helper overrides or missing V05.
- [x] V05 checks core V02 prerequisites, installs the current four-argument/eight-column claim, and drops the old helper overload. Reuse the exact generated eight-field definition from Task 2, with matching checksum; no second hand-maintained claim algorithm.

```sql
DROP FUNCTION IF EXISTS pgflow.start_tasks(text, bigint[], uuid);
-- Install the recorded eight-column four-argument function from Task 2.
```

  Historical helper V03 will still widen the type and create its obsolete function during a fresh helper-chain replay. V05 must repair the final four-argument definition and remove that obsolete function before the installation transaction commits. Prove SQL function validation succeeds at every intermediate DDL step, not just the final catalog.
- [x] Rebase recovery and pruning on task snapshots. Preserve configurable buffer, effective step timeout, active parent guards, requeue cap, SKIP LOCKED, and forced side-effect execution. In recovery select `st.queue_name`, return `tr.queue_name`, and archive by `ta.queue_name`. In pruning group message deletion by task queue, and derive archive routes from persisted steps/tasks before deleting task rows. Preserve flow filters and deletion counts.

```sql
SELECT st.queue_name, array_agg(st.message_id) AS message_ids
FROM pgflow.step_tasks st JOIN pgflow.runs r USING (run_id)
WHERE st.message_id IS NOT NULL
  AND (r.completed_at < $1 OR r.failed_at < $1)
GROUP BY st.queue_name;
```

- [x] Keep the shim, `flow_type`, indexes, worker helpers, and tracking objects intact. V05 downgrade also fails explicitly; do not reinstall a three-argument claim into a V02 database. Generate consumer upgrade wrappers that call core then helpers in one transaction. Explain that an already-applied Ecto wrapper does **not** rerun just because the dependency version changed; consumers need a new wrapper migration.
- [x] Run migration tests for the three routes, helper recovery tests, and core-only catalog tests. Verify fresh and upgraded final schemas agree apart from catalog OIDs and expected tracking metadata. Explicitly test recovery sweeps containing only exhausted tasks so archival cannot be skipped by an empty requeue result.

### Task 4: Adopt queue-aware claiming and SQL-owned lifecycle results

**Files:** Modify `lib/pgflow/queries/flows.ex`, `lib/pgflow/worker/server.ex`, `lib/pgflow/worker/task_row.ex`, `lib/pgflow/schema/step.ex`, `lib/pgflow/schema/step_task.ex`, `test/support/db/test_helpers.sql`, `test/support/integration_case.ex`; create `test/pgflow/upstream/queue_identity_test.exs`; update `test/pgflow/{queries_test,schema_alignment_test}.exs` and `test/pgflow/worker/task_row_test.exs`.

**Interfaces:** `Flows.start_tasks(repo, flow_slug, msg_ids, worker_id, queue_name)` requires an explicit queue. Worker state has separate `flow_slug` and `queue_name`. Keep native Elixir integers for bigint IDs. `TaskRow.decode/1` reads the final eight-column result; compatibility with seven columns must not silently substitute attempt 1 during supported startup.

- [x] Write a test using the same bigint message ID in two queues: claim one, assert the other task's attempts/status/message are unchanged. Add wrong-flow, unknown-message, duplicate-claim, null-queue, mixed-case, and ID `9007199254740993` cases. Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/upstream/queue_identity_test.exs` and verify RED.
- [x] Change the query and all callers, including test SQL helpers:

```elixir
sql = "SELECT * FROM pgflow.start_tasks($1::text,$2::bigint[],$3::uuid,$4::text)"
SQL.query(repo, sql, [flow_slug, msg_ids, parse_uuid(worker_id), queue_name])
```

  Poll through the canonical route used for the claim. Add `field(:queue_name, :string)` to both Ecto schemas, casts, and schema-alignment assertions. Keep flow slug casing intact. Current default route is lower(flow slug); route normalization must agree with PostgreSQL for all accepted slugs.
- [x] Update declined-message diagnostics to match exact stored queue/message identity and flow ownership. Preserve unmatched/wrong-flow messages and emit identifier-only warnings. Do not use message bodies to authorize cleanup. Remove the worker's redundant message deletion after successful `complete_task`; upstream SQL owns archival, including no-op late callbacks. Do not interpret any successful RPC transport as proof of a fresh terminal transition.
- [x] Run queue/worker tests plus upstream-inspired claim race tests. Assert only `UPDATE ... RETURNING` rows execute, attempts increment once, incomplete visibility update rolls back the claim, and duplicate/late callbacks cannot change a terminal run. Preserve valid work from a batch containing unmatched messages.

### Task 5: Serialize the exact upstream flow shape

**Files:** Create `lib/pgflow/flow/shape.ex`, `test/pgflow/flow/shape_test.exs`, `test/support/upstream/shape_cases.json`; modify `lib/pgflow/flow/step.ex` and `lib/pgflow/dsl/validation.ex` only as needed to represent supported JSON pattern values without ambiguity.

**Interfaces:** `PgFlow.Flow.Shape.from_definition(%PgFlow.Flow.Definition{}) :: map()` returns the upstream camelCase JSON shape. It preserves declaration order, sorts dependency names, converts `:skip_cascade` to `"skip-cascade"`, and represents missing versus JSON-null conditions explicitly. Additional `flow_type`/cron metadata is outside the upstream shape.

- [x] Add a failing golden case:

```elixir
assert Shape.from_definition(definition) == %{
  "steps" => [%{
    "slug" => "charge", "stepType" => "single", "dependencies" => ["approval"],
    "whenUnmet" => "skip", "whenExhausted" => "fail",
    "requiredInputPattern" => %{"defined" => false},
    "forbiddenInputPattern" => %{"defined" => false}
  }]
}
```

  Add fixtures for root/dependent maps, options including zero baseDelay/startDelay, all condition modes, arrays/scalars/null patterns, and jobs. Missing options remain omitted; zero must not become SQL NULL. For explicitly supplied null patterns retain option presence separately from its value; existing `nil` fields alone cannot express the difference.
- [x] Run `mix test test/pgflow/flow/shape_test.exs`; expect the missing serializer to fail. Implement extraction following pinned upstream `pkgs/dsl/src/flow-shape.ts` and SQL `_create_flow_from_shape`/`_compare_flow_shapes`. Record defaults and option naming in this module, not independently in each caller.

```elixir
defp mode(:skip_cascade), do: "skip-cascade"
defp mode(value) when value in [:skip, :fail], do: Atom.to_string(value)
defp pattern(false, _value), do: %{"defined" => false}
defp pattern(true, value), do: %{"defined" => true, "value" => value}
```

- [ ] Run the golden tests and SQL shape comparison tests. Expect structural changes to produce differences, while changes limited to runtime-tunable retry/timeout options do not. Task 10 runs TypeScript `extractFlowShape` against the same fixtures to prove correspondence.

### Task 6: Compile and verify before worker registration

**Files:** Create `lib/pgflow/worker/bootstrap.ex`, `test/pgflow/worker/bootstrap_test.exs`; modify `lib/pgflow/queries/{flows,workers}.ex`, `lib/pgflow/worker/server.ex`, `lib/pgflow/flow_starter.ex`, `lib/pgflow/worker_supervisor.ex`, `lib/pgflow/flow_compiler.ex`, `lib/pgflow/job_compiler.ex`, `lib/pgflow/client.ex`; update startup and runtime-management tests.

**Interfaces:** `Flows.ensure_flow_compiled(repo, slug, shape)` returns `{:ok, %{status: status, differences: differences}}` for compiled/verified/recompiled or `{:error, {:flow_shape_mismatch, differences}}`. `PgFlow.Worker.Bootstrap.prepare(repo, definition)` returns `{:ok, %{queue_name: queue_name, compilation_status: status}}` or an error. `WorkerQueries.track_worker_function(repo, name, "process")` calls upstream registration.

- [x] Add tests proving a missing flow compiles at worker startup, production mismatch never deletes runs or registers a worker, matching shape preserves runtime tuning, and a connectivity failure does not let polling begin. Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/worker/bootstrap_test.exs`; expect current existence-only behavior to fail.
- [x] Implement bootstrap with parameterized calls and a transaction for compilation plus Elixir metadata:

```sql
SELECT pgflow.ensure_flow_compiled($1::text, $2::jsonb);
-- After successful compilation/verification, preserve the definition's classification:
UPDATE pgflow.flows SET flow_type = $2::text WHERE flow_slug = $1::text;
-- Only after bootstrap succeeds:
SELECT pgflow.track_worker_function($1::text, 'process');
```

  Validate expected core/helper signatures and record shape before registering a supported Elixir worker; error clearly for an old or partially upgraded schema. Never fall back to the three-argument claim. Use `is_local()` unchanged. Test local destructive recompilation only in a fixture with the explicit upstream local setting, and prove job classification survives it.
- [x] Classify shape mismatch/unsupported schema as permanent in FlowStarter; connection failures remain retriable with existing backoff. Preserve the documented distinction between `ready?` (converged) and `healthy?`; do not silently redefine it. Tests awaiting execution must require successful module startup, not merely convergence.
- [x] Verify runtime tuning end to end: upstream excludes timeout/retry options from structural comparison, so a matching shape must not overwrite database tuning or run an OTP timeout inconsistent with the effective database value. Add `Flows.execution_options(repo, flow_slug)` returning options keyed by known step slug, using the query below. Refresh execution options for each claimed batch before starting handlers, fail closed on query failure, and test a step timeout changed between batches. SQL remains authoritative for retry decisions; logs must report the persisted attempt/outcome rather than reconstructing retry policy from stale module defaults.

```sql
SELECT s.step_slug,
       coalesce(s.opt_timeout, f.opt_timeout) AS timeout,
       coalesce(s.opt_max_attempts, f.opt_max_attempts) AS max_attempts,
       coalesce(s.opt_base_delay, f.opt_base_delay) AS base_delay
FROM pgflow.steps s JOIN pgflow.flows f USING (flow_slug)
WHERE s.flow_slug = $1::text;
```

  This does not promise atomic live reconfiguration of already-running handlers. Document that tuning applies to subsequent dispatches; use coordinated changes when altering active execution policy.
- [x] Preserve old generated migrations and explicit upsert behavior while documenting deprecation. Route destructive deletion through upstream `delete_flow_and_data` and use compatible advisory locking (`pg_advisory_xact_lock(1, hashtext(lower(slug)))` at this pin) so Elixir definition operations serialize with upstream. Never silently change `upsert_flow` into non-destructive verification under the same name.
- [x] Run bootstrap, FlowStarter, compiler, runtime-management and cron tests. Confirm cron schedules are not duplicated by worker restarts and an existing cron schedule remains operable after startup compilation. Generated migrations may still create definitions as an explicit Elixir compatibility extension; normal worker startup must always verify them.

### Task 7: Match lifecycle guarantees using OTP

**Files:** Modify `lib/pgflow/worker/{server,lifecycle}.ex`, `lib/pgflow/worker_supervisor.ex`, `lib/pgflow/queries/workers.ex`, `lib/pgflow/flow_starter.ex`; create `test/pgflow/worker/upstream_lifecycle_test.exs`; update existing lifecycle/server tests.

**Interfaces:** Keep public worker stop APIs. Implement draining through GenServer state and normal completion/DOWN/timeout messages, with deferred `GenServer.reply` to stop callers. Distinguish intentional stop from crash replacement. Heartbeat reports persisted deprecation and triggers draining before supervised replacement.

- [x] Add controlled-handler tests: a blocked task receives a release message after stop is requested, completes in SQL, then the stop caller returns. Assert no further polling starts during drain; test repeated stop requests, crash during drain, timeout, and startup failure. Use `assert_receive` and monitors rather than sleeping.

```elixir
send(worker, :poll_now)
assert_receive {:handler_started, handler_pid}
stopper = Task.async(fn -> PgFlow.Worker.Server.stop(worker) end)
send(handler_pid, :release)
assert :ok = Task.await(stopper)
assert run_status(repo, run_id) == "completed"
```

  Define `run_status/2` locally in this test using `SELECT status FROM pgflow.runs WHERE run_id=$1`. Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/worker/upstream_lifecycle_test.exs`; demonstrate any existing drain defect before fixing it.
- [x] Replace any blocking receive loop inside the stop callback with a stopping state that keeps processing handler outcomes. Store stop waiters, cancel poll timers, and finish only when active tasks are empty. A timeout/crash still reports through normal failure SQL. Abrupt process/application shutdown must leave durable tasks recoverable if graceful draining cannot finish.

```elixir
# State transition outline inside handle_call(:stop, from, state):
state = %{state | lifecycle: Lifecycle.transition!(state.lifecycle, :stopping),
                  stop_waiters: [from | state.stop_waiters]}
# Return {:noreply, state} while work exists; result/DOWN/timeout callbacks
# update active_tasks and call the same finish-drain operation.
```

- [x] Respect `workers.deprecated_at` on heartbeat using upstream-equivalent `UPDATE ... RETURNING (deprecated_at IS NOT NULL)`. Missing registration is treated as deprecated. For replacement, drain the old worker, then let OTP start a new worker that reruns bootstrap. For explicit operator stop, terminate/remove the supervised child so a permanent restart policy does not immediately undo the stop. Do not rely on DynamicSupervisor child IDs, which are not retained as lookup identities; use a deliberate module-to-worker registration or maintained registry.
- [x] Preserve `min(available_slots, batch_size)` and eager refill. Verify externally supplied Ecto repos remain running after stop. Test process-mode registration is excluded from HTTP ensure-workers on a Supabase fixture. Do not treat `worker_functions.enabled=false` as an upstream process pause API; operators must stop the owning OTP application/supervision path during upgrade.
- [x] Run lifecycle, worker and recovery tests. A normal stop leaves completed handler results committed; a forced crash leaves messages available to the SQL recovery rules. No new platform-adapter abstraction is needed.

### Task 8: Update every operational queue route

**Files:** Modify `lib/pgflow/queries/flows.ex`, `lib/pgflow/runs.ex`, `lib/pgflow/workers.ex`, `lib/pgflow/flow_starter.ex`, `lib/pgflow/signal/notify.ex`; create `test/pgflow/upstream/operational_routes_test.exs`; update `test/pgflow/{runs,workers,client}_test.exs` and notify tests.

**Interfaces:** Public run/job APIs keep their signatures. Internally derive per-run message operations from persisted `step_tasks.queue_name`; definition/archive routes use persisted steps and the canonical default for empty flows. Flow filters use flow identity, not accidental equality with worker queue names.

- [x] Seed internal multi-route fixtures, with identical message IDs in different queues, and test delay, make-available, count, run deletion, flow deletion, prune, worker health and notifications. Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/upstream/operational_routes_test.exs`; expect old flow-derived routing to fail.
- [x] Select queue alongside message IDs in delayed jobs and group mutation work by queue. For counts/deletes use task queue+message identity, not JSON message envelopes. Resolve actual physical table identifiers with PGMQ's formatting function and SQL identifier quoting; never interpolate unchecked slugs.

```sql
SELECT queue_name, array_agg(message_id) AS message_ids
FROM pgflow.step_tasks
WHERE run_id = $1::uuid AND message_id IS NOT NULL
GROUP BY queue_name;
```

  Collect needed routes before deleting task records. Let upstream flow deletion resolve legacy listed queue spelling and enforce ambiguity/collision checks; avoid an independent Elixir deletion algorithm. Update worker-to-flow joins to use persisted routes without duplicate worker rows when multiple steps share a queue.
- [x] Register notification wakeups using the same canonical queue as polling, while keeping module/flow lookup independent. Confirm notify is optional and missed notifications are recovered by polling. Test mixed-case queues without creating new queue metadata entries.
- [x] Run operational, cron, delayed-job, notify and helper-pruning suites. Assert counts/filter scopes and return structs remain unchanged and no unrelated queue messages are removed.

### Task 9: Preserve JSON and surface new task statuses accurately

**Files:** Modify `lib/pgflow/worker/server.ex`, `lib/pgflow/context.ex`, `lib/pgflow/client.ex`, `lib/pgflow/queries/flows.ex`, `lib/pgflow/telemetry.ex`, `lib/pgflow_dashboard/components/status_badge.ex`, `lib/pgflow_dashboard/live/runs_live/show.ex`; create `test/pgflow/upstream/json_contract_test.exs`; update conditional-step/dashboard/worker tests where their expectations encode old behavior.

**Interfaces:** Elixir handlers keep their established return convention, while valid JSON output retains its value. Task statuses include `skipped` and `cancelled`; run statuses remain upstream's existing set. Do not invent cancelled runs or claim process cancellation when only the durable task has terminalized.

- [x] Add round-trip tests for maps, arrays, strings, integers, fractional numbers, `false`, and `nil`, including map output aggregation and `Context.get_flow_input/1`. Assert JSON scalars are not wrapped in `{"_raw": ...}`. Run `PGFLOW_REQUIRE_DB=1 mix test test/pgflow/upstream/json_contract_test.exs`; expect current scalar handling to fail.
- [x] Validate encoding while passing the original JSON term to Postgrex. Invalid output must fail the task with a stable redacted serialization error, not complete successfully with an inspection of a PID/struct or log arbitrary payloads.

```elixir
case Jason.encode(output) do
  {:ok, _encoded} -> {:ok, output}
  {:error, _reason} -> {:error, "Handler output is not JSON encodable"}
end
```

  Generalize context input guards/types so false, null, arrays, and scalars are cached values distinct from `:not_loaded`. Keep safe mapping of known dependency names to existing atoms; never mint atoms from foreign SQL data.
- [x] Render skipped/cancelled tasks distinctly and prevent late callback telemetry from announcing new completion/failure after SQL has declined a transition. Preserve completed siblings and the failed culprit while unfinished siblings become cancelled. Historical dashboard SQL stays immutable; if a persisted view/function needs updating, add dashboard V04 with its own migration tests.
- [x] Run JSON, conditional-step, telemetry and affected dashboard tests. Record any upstream TypeScript adapter discrepancy: at the research pin `PgflowSqlClient.completeTask` uses `output || null`, which may turn false/zero/empty-string into null. Task 10 must test and report this precisely; do not weaken the SQL/Elixir JSON assertions to match it.

### Task 10: Prove SQL and cross-language compatibility

**Files:** Create `test/support/upstream/{run.exs,worker.mjs,shape_cases.mjs}`, `test/pgflow/upstream/interoperability_test.exs`, `.github/workflows/upstream-compatibility.yml`; extend `test/support/upstream/manifest.json` and fixtures from Task 1.

**Interfaces:** `mix run --no-start test/support/upstream/run.exs --checkout PATH --sha SHA --database-url URL --suite SUITE` supports `catalog`, `pgtap`, `typescript`, and `all`. It rejects non-fixture targets, wrong source SHA, missing prerequisites, and silently skipped suites. It records source revision, package versions, test counts and failures without credentials. The harness is test-only, not a production dependency.

- [x] Add a failing interoperability case: a real pinned TypeScript `PgflowSqlClient` starts a flow on the Elixir-installed database and an OTP worker completes it; then reverse the producer/worker languages using equivalent definitions. Test each runtime independently first, then concurrent workers for the same compatible flow. Do not claim heterogeneous per-step worker routing, which this pin does not ship.
- [x] Build pinned upstream packages with its lockfile in a dedicated checkout. Use its exported Node process worker API as documented by the pinned source; do not patch upstream production files or publish packages. The worker fixture uses environment connection configuration and disposes only resources it owns. TypeScript test calls retain exact decimal-string bigint IDs:

```typescript
const claimed = await client.startTasks(flowSlug, ["9007199254740993"], workerId, queueName);
if (claimed.some(task => task.msg_id !== "9007199254740993")) {
  throw new Error("message ID lost precision");
}
```

- [ ] Run upstream pgTAP lifecycle and identity cases on the **Elixir-installed** schema. Use pinned `pkgs/core/supabase/seed.sql`/test helpers with only documented fixture adaptations. Include `start_tasks`, `queue_identity`, `fail_task_when_exhausted`, `requeue_stalled_tasks`, `complete_task`, condition terminalization, creation/deletion, and prune cases. Upstream's optional one-argument pruning fixture is not equivalent to our filtered two-argument helper: test both through their intended installation profiles.
- [x] Compare final core catalogs/functions against a pristine upstream install. Allow only recorded portability omissions, `flow_type`, added indexes/tracking objects/helpers, parameter-name differences explicitly reconciled, and the attempt-count record/function extension. An unexplained difference fails the gate. Do not globally ignore all helper-overridden core functions.
- [ ] Run the populated upgrade matrix, rollback-on-conflict cases, JSON false/null probes, shape golden fixtures, and claim/terminal races using separate database connections. SQL Sandbox tests sharing one transaction are insufficient for lock-race evidence. Test both plain PostgreSQL with shim and Supabase-compatible fixtures with real realtime preserved. Exercise the Supabase HTTP/process distinction only where its infrastructure exists and report the profile separately.
- [x] Add CI running this harness with `PGFLOW_REQUIRE_DB=1`, isolated names/ports and pinned dependencies. Invoke:

```sh
mix run --no-start test/support/upstream/run.exs --checkout "$PGFLOW_UPSTREAM_CHECKOUT" --sha 94490709f79ebf366141dd925b047f0c1013e759 --database-url "$PGFLOW_COMPAT_DATABASE_URL" --suite all
PGFLOW_REQUIRE_DB=1 mix test
PGFLOW_REQUIRE_DB=1 mix test --only migration
mix quality
```

  Expected: every required profile passes, with no integration exclusions disguised as success. If the pinned upstream falsy-output case fails, record exact actual/expected behavior and mark that compatibility claim unresolved; do not claim universal JSON parity or silently modify the pin. Review the resulting scoped upstream fix/revision decision before release.

### Task 11: Document the maintenance upgrade and complete release evidence

**Files:** Create `docs/UPSTREAM_COMPATIBILITY.md`, `docs/UPGRADING_UPSTREAM_2026_09.md`; modify `README.md`, `docs/{ARCHITECTURE,ELIXIR_VS_SUPABASE,DASHBOARD}.md`, `lib/pgflow.ex`, `lib/mix/tasks/{pgflow.setup,pgflow.check_schema}.ex`; add focused generator/check-schema tests under `test/mix/tasks/`. Complete demo files through companion tasks D1–D4.

**Interfaces:** Compatibility reporting names the upstream SHA and local core/helper versions. Keep existing public `PgFlow.core_version/0` return type stable; explain the release/SHA distinction instead of claiming 0.17.0 has been released. `mix pgflow.check_schema` verifies current signatures, queue constraints and helper record layout.

- [x] Add tests rejecting old and partial schemas and proving generated upgrade wrappers call core then helpers. Run focused mix-task tests; expect the old checker/generator to miss the new contract.
- [x] Implement the checks and generated wrapper using:

```elixir
def up do
  PgFlow.Migration.up()
  PgFlow.HelpersMigration.up()
end

def down do
  raise "This PgFlow upstream upgrade is forward-only; restore the coordinated backup to revert"
end
```

  State that wrapper transaction ownership is required and show a new migration timestamp for existing installations. Never advise rerunning a stamped old wrapper as the upgrade mechanism.
- [x] Write the operator procedure in this order: identify/backup the target database; record worker enablement and running applications; pause producers; drain handlers under the old schema without emptying queues; stop OTP applications/process workers and HTTP re-invocation; stop recovery/pruning/definition writers; apply core+helpers transaction; verify signatures/backfill/queue preservation; deploy matching callers; restore maintenance and exact saved worker states; resume producers. A failed migration permits retry under the old schema after rollback; a successful migration does not permit old workers to restart.
- [x] Document known overlays, startup-only upstream versus retained Elixir compatibility APIs, job/cron preservation, version-floor requirements, schema permissions, queue exclusivity, SQL-owned archival, new task statuses, and the absence of external waits/per-step queues. Explain plain-Postgres `is_local()` behavior explicitly. Preserve public method names except the required low-level claim argument, and list every intentional behavior correction such as JSON scalar preservation.
- [ ] Run `mix quality`, full DB-backed tests, isolated migration tests and the Task 10 harness once after final code changes. Record actual commands/counts and any failing profile in this document's execution ledger. Review code and spec coverage before proposing a release. Updating package version, tagging, committing, publishing, pushing, or changing host applications remains a separate authorized action.

## Acceptance checklist

- [x] Core V01 and helpers V01–V04 are byte-for-byte unchanged.
- [x] Upstream source SHA, source file hashes and all transformations are reproducible.
- [x] Fresh, existing V01+V04, and core-only installation routes converge correctly.
- [x] Four-argument claim is the sole final claim overload and returns the real attempt count.
- [x] All message mutations, maintenance paths and operational joins respect persisted queue identity.
- [x] Claims, visibility rollback, late callbacks, skip/cancel, and stalled recovery pass real concurrency tests.
- [x] Startup validates schema/shape before registration; production mismatch preserves history.
- [x] OTP drain commits in-flight outcomes; explicit stop stays stopped; crashes/deprecation have tested replacement behavior.
- [x] Jobs, cron, notification fallback, dashboard and externally owned repos retain their contracts.
- [ ] Demo D1–D4 pass: populated upgrade, feature catalogue, reconnect/restart behavior, browser verification, demo precommit and assets build.
- [x] Pinned TypeScript and Elixir execute against the same database with recorded profile results and JSON limitations.
- [x] Upgrade instructions require coordinated deployment and a new consumer Ecto wrapper.
- [x] External waits remain a separate follow-up after this acceptance gate.

## Execution ledger

Planning complete on 2026-09-14. No implementation or verification gate above has run as part of writing this document. During execution, append each task's changed files, RED/GREEN evidence, review findings, and remaining blockers here; check its boxes only after the corresponding evidence exists.

Completion checkbox audit performed on 2026-09-14 against the final remediation reports and logs. See `.superpowers/sdd/plan-completion-audit.md` for the evidence map and the exact reasons that the TypeScript shape, pgTAP creation/deletion, full compatibility-matrix, final-after-all-changes, and aggregate demo gates remain unchecked.
