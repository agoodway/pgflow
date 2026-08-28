# PgFlow Core Operational Queries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move operational run, worker, definition, cron, and metrics access into typed PgFlow core APIs, delete Dashboard query modules, and adopt the APIs in the demo and Appraisal Inbox.

**Architecture:** Repository-explicit core domain modules query `pgflow.*` with Ecto and return typed schemas or calculated structs. Dashboard and consumers call those modules directly; raw SQL is retained only for validated dynamic PGMQ queue cleanup and visibility operations.

**Tech Stack:** Elixir 1.19, Ecto SQL, PostgreSQL, PGMQ, Phoenix LiveView, ExUnit.

## Global Constraints

- Do not commit, stage, unstage, tag, push, publish, stash, reset, or clean any repository.
- Use test-driven development: add a failing test, verify the expected failure, implement the minimum behavior, and rerun green before refactoring.
- Delete `PgFlowDashboard.Queries.*` immediately; do not add deprecated wrappers or delegation modules.
- Return typed structs for every table-backed record and calculated projection.
- Keep historical Dashboard migration SQL immutable; runtime code must not depend on the Dashboard schema.
- Preserve unrelated work in every checkout.
- Run formatting and focused tests after each task; run full quality and test gates before completion.

---

### Task 1: Align PgFlow schemas and support arbitrary JSON

**Files:**
- Create: `lib/pgflow/type/json.ex`
- Modify: `lib/pgflow/schema/run.ex`
- Modify: `lib/pgflow/schema/step_state.ex`
- Modify: `lib/pgflow/schema/step_task.ex`
- Modify: `lib/pgflow/schema/worker.ex`
- Modify: `lib/pgflow/schema/flow.ex`
- Test: `test/pgflow/type/json_test.exs`
- Test: `test/pgflow/schema_alignment_test.exs`

**Interfaces:**
- Produces: `PgFlow.Type.JSON`, corrected `Run`, `StepState`, `StepTask`, `Worker`, and `Flow` structs.

- [x] **Step 1: Write failing JSON and schema-alignment tests**

  Assert that `PgFlow.Type.JSON` round-trips maps, lists, strings, numbers,
  booleans, and nil. Assert the schema field lists and types match the columns
  used by PgFlow SQL, including `StepTask.attempts_count`,
  `StepTask.last_worker_id`, and `Worker.last_heartbeat_at`, while excluding the
  non-existent `StepTask.input`.

- [x] **Step 2: Run the focused tests and verify RED**

  Run: `mix test test/pgflow/type/json_test.exs test/pgflow/schema_alignment_test.exs`

  Expected: failure because `PgFlow.Type.JSON` and corrected fields do not yet
  exist.

- [x] **Step 3: Implement the JSON type and align schemas**

  `PgFlow.Type.JSON` implements `Ecto.Type` with `type/0` returning `:map` and
  permissive cast/load/dump clauses for Jason-compatible values. Replace JSON
  `:map` fields that may contain arrays or scalars with this type and make every
  schema reflect the persisted table.

- [x] **Step 4: Run focused tests and formatting**

  Run: `mix test test/pgflow/type/json_test.exs test/pgflow/schema_alignment_test.exs && mix format`

  Expected: all focused tests pass.

### Task 2: Add typed operational result structs

**Files:**
- Create: `lib/pgflow/run_summary.ex`
- Create: `lib/pgflow/worker_summary.ex`
- Create: `lib/pgflow/definition_summary.ex`
- Create: `lib/pgflow/cron_summary.ex`
- Create: `lib/pgflow/run_history_cell.ex`
- Create: `lib/pgflow/overview_metrics.ex`
- Test: `test/pgflow/operational_structs_test.exs`

**Interfaces:**
- Produces: public structs with enforced keys, typespecs, and constructors from
  query projections.

- [x] **Step 1: Write failing construction tests**

  Exercise every struct constructor with the complete projection shape and
  assert field access, UUID strings, timestamps, decimals, and count fields.

- [x] **Step 2: Run the focused test and verify RED**

  Run: `mix test test/pgflow/operational_structs_test.exs`

  Expected: failure because the result modules do not exist.

- [x] **Step 3: Implement focused typed structs**

  Each module defines `@enforce_keys`, `defstruct`, `@type t`, and a small
  `new/1` constructor. Do not introduce a generic map-to-struct utility.

- [x] **Step 4: Run the focused test and formatting**

  Run: `mix test test/pgflow/operational_structs_test.exs && mix format`

  Expected: all tests pass.

### Task 3: Implement typed run reads and filters

**Files:**
- Create: `lib/pgflow/runs.ex`
- Modify: `lib/pgflow/client.ex`
- Test: `test/pgflow/runs_test.exs`
- Test: `test/pgflow/client_test.exs`

**Interfaces:**
- Produces: `PgFlow.Runs.get/2`, `get_with_states/2`, `list/2`, `count/2`,
  `list_step_states/2`, `list_step_tasks/3`, `get_step_task/4`, `adjacent/3`, and
  `history/3`.

- [x] **Step 1: Write failing run-query tests**

  Cover valid and invalid UUIDs, not-found results, list-valued step output,
  deterministic cursor ordering, flow/status/type/time filters,
  `input_contains`, state ordering, complete task fields, adjacent navigation,
  and typed history cells.

- [x] **Step 2: Run the focused tests and verify RED**

  Run: `mix test test/pgflow/runs_test.exs`

  Expected: failures for the missing `PgFlow.Runs` API.

- [x] **Step 3: Implement Ecto run queries**

  Build filters with composable private functions over `Ecto.Query`; use
  PostgreSQL JSONB containment for `input_contains`; select schemas for table
  reads and explicit projection maps converted to typed summary structs for
  calculated reads. Return error tuples rather than swallowing repository
  errors.

- [x] **Step 4: Route existing client reads through the corrected query path**

  Keep the established `PgFlow.get_run/1` and `get_run_with_states/1` public
  behavior while making `Client` use `PgFlow.Runs` with the configured repo.

- [x] **Step 5: Run focused tests and formatting**

  Run: `mix test test/pgflow/runs_test.exs test/pgflow/client_test.exs && mix format`

  Expected: all focused tests pass, including list-valued output.

### Task 4: Implement run visibility and transactional deletion

**Files:**
- Modify: `lib/pgflow/runs.ex`
- Test: `test/pgflow/run_lifecycle_test.exs`

**Interfaces:**
- Produces: `PgFlow.Runs.make_available/2` and idempotent
  `PgFlow.Runs.delete/2`.

- [x] **Step 1: Write failing lifecycle tests**

  Create delayed single and map runs, prove messages are initially invisible,
  then assert `make_available/2` exposes every queued task. Populate both live
  and archived queue rows, call `delete/2`, and assert messages, tasks, states,
  and run rows are absent. Repeat deletion to prove idempotency. Cover invalid
  IDs, invalid stored slugs, and missing queues.

- [x] **Step 2: Run the lifecycle test and verify RED**

  Run: `mix test test/pgflow/run_lifecycle_test.exs`

  Expected: failures for missing lifecycle functions.

- [x] **Step 3: Implement visibility and deletion**

  Use a repository transaction and `FOR UPDATE` run lock. Validate the stored
  slug with PgFlow's established slug rules before interpolating identifiers.
  Delete matching JSON `run_id` messages from both PGMQ tables, then delete
  persisted rows in foreign-key order. Treat absent runs and queues as success;
  return all other database errors.

- [x] **Step 4: Run focused tests and formatting**

  Run: `mix test test/pgflow/run_lifecycle_test.exs && mix format`

  Expected: all lifecycle tests pass.

### Task 5: Implement typed worker operations

**Files:**
- Create: `lib/pgflow/workers.ex`
- Modify: `lib/pgflow/queries/workers.ex`
- Test: `test/pgflow/workers_test.exs`

**Interfaces:**
- Produces: `PgFlow.Workers.get/2`, `list/2`, `count/2`, `list_tasks/3`,
  `adjacent/3`, and `delete/2`.

- [x] **Step 1: Write failing worker tests**

  Cover typed worker reads, flow and health filters, deterministic cursors,
  active/completed task counts, tasks owned by a worker, adjacent ordering,
  invalid IDs, and idempotent deletion.

- [x] **Step 2: Run the focused test and verify RED**

  Run: `mix test test/pgflow/workers_test.exs`

  Expected: failure because `PgFlow.Workers` does not exist.

- [x] **Step 3: Implement worker operations**

  Use Ecto queries over the corrected schemas. Keep the existing low-level
  registration and heartbeat commands in `PgFlow.Queries.Workers`; do not
  duplicate them.

- [x] **Step 4: Run focused tests and formatting**

  Run: `mix test test/pgflow/workers_test.exs test/pgflow/queries_test.exs && mix format`

  Expected: all focused tests pass.

### Task 6: Implement stored definitions, crons, and metrics

**Files:**
- Create: `lib/pgflow/definitions.ex`
- Create: `lib/pgflow/metrics.ex`
- Test: `test/pgflow/definitions_test.exs`
- Test: `test/pgflow/metrics_test.exs`

**Interfaces:**
- Produces: the `PgFlow.Definitions` and `PgFlow.Metrics` APIs specified in the
  design.

- [x] **Step 1: Write failing definition and metrics tests**

  Cover stored flow and step retrieval with retry/conditional policies,
  flow/job filtering, 24-hour statistics, cron joins and last-run state,
  pagination, not-found behavior, and every overview metric.

- [x] **Step 2: Run focused tests and verify RED**

  Run: `mix test test/pgflow/definitions_test.exs test/pgflow/metrics_test.exs`

  Expected: failures for missing modules.

- [x] **Step 3: Implement typed Ecto projections**

  Query `pgflow.flows`, steps, deps, runs, workers, tasks, and `cron.job`
  directly. Keep cron next-run calculation in core but leave human-readable
  schedule copy in Dashboard presentation code.

- [x] **Step 4: Run focused tests and formatting**

  Run: `mix test test/pgflow/definitions_test.exs test/pgflow/metrics_test.exs && mix format`

  Expected: all focused tests pass.

### Task 7: Delete Dashboard queries and migrate every caller

**Files:**
- Delete: `lib/pgflow_dashboard/queries/crons.ex`
- Delete: `lib/pgflow_dashboard/queries/flows.ex`
- Delete: `lib/pgflow_dashboard/queries/jobs.ex`
- Delete: `lib/pgflow_dashboard/queries/metrics.ex`
- Delete: `lib/pgflow_dashboard/queries/runs.ex`
- Delete: `lib/pgflow_dashboard/queries/workers.ex`
- Modify: `lib/pgflow_dashboard/live/**/*.ex`
- Create or modify: `lib/pgflow_dashboard/live/cron_presentation.ex`
- Modify: `test/pgflow_dashboard/**/*.exs`

**Interfaces:**
- Consumes: all typed core modules from Tasks 1-6.
- Produces: Dashboard code with no `PgFlowDashboard.Queries` references and no
  runtime dependency on `pgflow_dashboard.*` RPCs.

- [x] **Step 1: Add failing architectural and LiveView tests**

  Add a source-boundary test that rejects `PgFlowDashboard.Queries` modules and
  runtime SQL references. Update LiveView expectations to typed structs before
  changing production callers.

- [x] **Step 2: Run Dashboard tests and verify RED**

  Run: `mix test test/pgflow_dashboard`

  Expected: failures while LiveViews still use Dashboard query modules.

- [x] **Step 3: Replace every Dashboard caller and delete query modules**

  Alias core domain modules directly. Adapt success/error handling to the core
  tuple contracts and use struct field access. Move only cron humanization and
  activity-grid grouping into presentation modules/components.

- [x] **Step 4: Prove the cut is complete**

  Run: `rg -n 'PgFlowDashboard\\.Queries|pgflow_dashboard\\.(list_|count_|get_)' lib demo test`

  Expected: no runtime call-site matches.

- [x] **Step 5: Run Dashboard tests and formatting**

  Run: `mix test test/pgflow_dashboard && mix format`

  Expected: all Dashboard tests pass.

### Task 8: Refactor the bundled demo

**Files:**
- Modify: `demo/lib/**/*.ex`
- Modify: `demo/test/**/*.exs`

**Interfaces:**
- Consumes: typed core APIs.
- Produces: demo application and tests without avoidable direct PgFlow SQL.

- [x] **Step 1: Identify and convert one test to the desired core API**

  Replace the first direct run/state/task read with the wished-for core call,
  then run that test to verify it fails before its supporting conversion is
  complete.

- [x] **Step 2: Replace remaining avoidable operational SQL**

  Preserve schema-migration contract SQL and deliberate low-level database
  tests. Replace application and behavior-test reads for runs, states, tasks,
  workers, definitions, and cleanup.

- [x] **Step 3: Run demo verification**

  Run from `demo/`: `mix format && mix test && mix quality`

  Expected: format, complete test suite, and quality pass.

### Task 9: Adopt the local PgFlow checkout in Appraisal Inbox

**Files:**
- Modify: `/Users/chasepursley/Development/af/appraisal_flow/inbox/server/mix.exs`
- Modify: `/Users/chasepursley/Development/af/appraisal_flow/inbox/server/mix.lock`
- Modify: `/Users/chasepursley/Development/af/appraisal_flow/inbox/server/test/support/pgflow_fixtures.ex`
- Modify: `/Users/chasepursley/Development/af/appraisal_flow/inbox/server/test/support/import_flow_fixtures.ex`
- Modify: related Inbox PgFlow tests found by the raw-query audit.

**Interfaces:**
- Consumes: local PgFlow path dependency and Tasks 3-6 APIs.
- Produces: Inbox tests without handwritten PgFlow operational queries where a
  core API exists.

- [x] **Step 1: Point Inbox at the local checkout and refresh only PgFlow**

  Use `{:pgflow, path: "../../../../os/pgflow"}` and run `mix deps.get`. Inspect the
  lockfile diff to ensure unrelated dependencies did not change.

- [x] **Step 2: Convert one fixture behavior and verify RED**

  Change the test expectation to use a new typed PgFlow result before replacing
  its helper implementation. Run the focused file and confirm the expected
  mismatch.

- [x] **Step 3: Replace PgFlow fixture SQL**

  Use core worker existence/deletion, task inspection, visibility, run cleanup,
  state listing, and input-filtered counts. Keep only application-specific run
  discovery SQL or database-context queries.

- [x] **Step 4: Audit all Inbox PgFlow test SQL**

  Run: `rg -n 'pgflow\\.|pgmq\\.|PgFlowDashboard\\.Queries|Ecto\\.Adapters\\.SQL' test --glob '*.{ex,exs}'`

  Classify every match as core-replaceable, app-specific, migration contract,
  or intentional low-level invariant. Replace every core-replaceable match.

- [x] **Step 5: Run focused Inbox PgFlow/email tests**

  Run all modified test files plus the PgFlow fixture consumers.

  Expected: all focused tests pass with no unused aliases or warnings.

### Task 10: Cross-repository verification and release handoff

**Files:**
- Modify only files needed to fix failures introduced by Tasks 1-9.

**Interfaces:**
- Produces: verified uncommitted work ready for explicit commit/release approval.

- [ ] **Step 1: Verify PgFlow core and Dashboard**

  Run from PgFlow root: `mix format && mix quality && mix test`.

- [ ] **Step 2: Verify the demo again after the root gate**

  Run from `demo/`: `mix format && mix quality && mix test`.

- [ ] **Step 3: Verify Inbox**

  Run from Inbox server: modified tests, `mix format`, `mix quality`, then the
  complete `MIX_ENV=test mix test` suite.

- [ ] **Step 4: Check repository boundaries and diffs**

  Run `git diff --check`, inspect `git status --short`, confirm no Dashboard
  query modules/callers remain, and confirm no unrelated dependency drift.

- [ ] **Step 5: Stop before Git or release mutations**

  Report exact tests, quality results, residual risks, and proposed semantic
  version. Wait for explicit permission before committing PgFlow, tagging,
  pushing, or publishing a release.
