# PgFlow Demo Upstream Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:subagent-driven-development only if delegation is authorized. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `/demo` with the synchronized library and make it a repeatable test bed and feature showcase for supported PgFlow capabilities.

**Architecture:** The existing Phoenix app consumes the parent checkout, runs actual PostgreSQL workflows, and displays durable results through public PgFlow APIs. A small scenario catalogue connects runnable definitions, inputs, expected outcomes, source examples, and tests. Keep app-specific scenario/UI code in the demo and orchestration behavior in the library.

**Tech Stack:** Phoenix 1.8, LiveView, Ecto, PgFlow, PostgreSQL/PGMQ/pg_cron, ExUnit/LiveViewTest, existing Tailwind/esbuild pipeline.

## Global constraints

- This companion is part of [the upstream alignment release](2026-09-14-upstream-typescript-alignment.md), not optional follow-up work. Parent-plan constraints apply.
- Read `demo/AGENTS.md` before implementation. Run its required `mix precommit`; inspect any dependency unlock changes from that alias and preserve unrelated dependency state.
- Keep the current article workflow, onboarding scenarios, cron cleanup example, graph, source display, and dashboard links usable.
- New baseline scenarios require no external network, paid API, mail credentials, or LLM key. Retain the existing article/LLM path as an explicitly configured integration example.
- Demonstrate every supported feature family with an executable scenario or operational walkthrough. Do not promise every combinatorial configuration or present future manual tasks/custom queues as implemented.
- Dangerous demonstrations such as worker crashes or deliberate mismatches run only in a dedicated test-bed profile against demo-owned runs/workers. They must not operate on arbitrary caller-provided identifiers or appear enabled on a public demo by default.
- No automatic deploy, publish, commit, staging, or production database operation. Preserve historical demo migrations; add a new upgrade migration.
- Keep event subscriptions scoped to selected run, reconcile from the database after reconnect, and bound log/history collections. Notifications are not the state of record.
- Use existing assets and components; no new frontend framework or external script tags. Follow Phoenix layout/form/stream conventions in `demo/AGENTS.md`.

## Existing baseline and coverage target

At `9dbaaaf`, the app has `PgflowDemo.Flows.ArticleFlow`, `PgflowDemo.Flows.OnboardingFlow`, `PgflowDemo.Jobs.ArticleFlowCleanup`, and one large `PgflowDemoWeb.FlowDemoLive`. The await branch's Job/Approval tabs are not in this baseline and must not be assumed present. Dev/test uses `{:pgflow, path: ".."}`; production uses a Hex requirement. The current README recommends generating setup/definition migrations even though historical migrations are already committed; correct that setup path.

| Feature family | Scenario or walkthrough | Observable acceptance |
| --- | --- | --- |
| Sequential and parallel DAG, fan-in | Existing article example plus deterministic `ParallelFlow` | Downstream starts only after both dependencies complete; no API key needed for baseline |
| Root map, dependent map, map→map, empty maps | `MapFlow` and `RootMapFlow`, preset normal/empty/scalar-item inputs | Correct indexed tasks, stable aggregate order, empty propagation |
| Conditional steps | Existing onboarding; add `PolicyFlow` presets for if/if_not and each when_unmet mode | Skipped branches, skip-cascade, failure and downstream eligibility match SQL |
| Retry, backoff, attempt context | `RetryFlow`, deterministic fail-until-attempt input | Attempts increase in displayed context; success after configured failures |
| Retry exhaustion | `ExhaustionFlow` presets for fail/skip/skip-cascade | Culprit/sibling terminal statuses and final run outcome match policy |
| Effective timeout and start delay | `TimeoutFlow` and `DelayedFlow` | Step override takes effect; retry/terminal result and delayed visibility are shown |
| JSON values | `JsonFlow`, scalar/list/object/false/null presets | Displayed output equals stored JSON without wrapping/coercion |
| Single-step jobs | `RecordJob` | Immediate enqueue, delayed enqueue, enqueue-at and sync caller examples |
| Cron flows and jobs | Existing cleanup job plus opt-in `ScheduledFlow` | Enable/disable, schedule, next run and history; no duplicate schedules after restart |
| Recovery and OTP lifecycle | Controlled `RecoveryFlow` operational walkthrough | Capacity freed, drain preserves result, crash recovered, replacement visible |
| Queue identity | Mixed-case `QueueIdentityFlow` and run details | Logical slug and physical queue differ visibly; run survives upgrade |
| Observability and operations | Existing PgFlow dashboard and history | Tasks include skipped/cancelled, worker health, counts, outputs, retry/prune/delete walkthrough |
| Startup compilation | New definitions have no generated definition migration | Starts and verifies after library migration; mismatch fails before polling |
| Multi-language compatibility | Link the real parent-plan interoperability run/report | Identify tested runtime pair/pin; do not invent a browser simulation |
| External waits and private per-step queues | Clearly labelled future work in documentation | No active controls or source suggesting these features are available |

## File map

- `demo/lib/pgflow_demo/scenarios.ex`: finite catalogue and parameter validation; no atom creation from browser input.
- `demo/lib/pgflow_demo/scenario_runner.ex`: starts/loads demo runs through public library APIs, associates scenario IDs with results, and scopes test-bed operations.
- `demo/lib/pgflow_demo/flows/{parallel_flow,map_flow,root_map_flow,policy_flow,retry_flow,exhaustion_flow,timeout_flow,delayed_flow,json_flow,recovery_flow,queue_identity_flow,scheduled_flow}.ex`: one module per definition; deterministic feature examples.
- `demo/lib/pgflow_demo/jobs/record_job.ex`: local no-side-effect job example alongside existing cleanup.
- `demo/lib/pgflow_demo_web/live/scenarios_live.ex`: feature catalogue/runner UI; leave article-specific UI in the existing LiveView.
- `demo/lib/pgflow_demo_web/components/scenario_source.ex`: highlighted source/examples using the current highlighting pipeline.
- `demo/test/pgflow_demo/{scenarios_test,scenario_runner_test,upstream_upgrade_test}.exs`: real execution and migration acceptance.
- `demo/test/pgflow_demo_web/live/scenarios_live_test.exs`: UI controls, switching, subscriptions/reconciliation, and status display.
- `demo/docs/SCENARIOS.md`: feature matrix with runnable inputs, outputs, and links to tests.

### Task D1: Upgrade the demo as a real consumer

**Depends on:** Parent Tasks 1–6.

**Files:** Modify `demo/mix.exs`, `demo/config/{test,runtime}.exs`, `demo/lib/pgflow_demo/application.ex`, `demo/README.md`; create an upgrade migration via the demo's required `mix ecto.gen.migration upgrade_pgflow_upstream_alignment`; create `demo/test/pgflow_demo/upstream_upgrade_test.exs`. Record its generated timestamp/path in the execution ledger.

**Interfaces:** Existing databases receive a new wrapper calling latest core and helpers; fresh databases replay existing migrations then the wrapper. The demo uses parent-checkout code in dev/test and an explicit local verification profile for release-mode checks.

- [x] Add a populated demo upgrade test with existing article/onboarding runs, cleanup job classification, a cron record, queued work and historical dashboard schema. Assert rows/outputs/queue messages survive and version/signature checks advance. Run from `demo`: `mix test test/pgflow_demo/upstream_upgrade_test.exs`; expect the missing upgrade wrapper to fail.
- [x] Generate the migration and implement:

```elixir
def up do
  PgFlow.Migration.up()
  PgFlow.HelpersMigration.up()
end

def down do
  raise "Coordinated PgFlow upgrade is forward-only; use the documented restore procedure"
end
```

  If parent Task 9 adds dashboard V04, include its upgrade in this same wrapper after helpers. Never rewrite old setup, compile, or dashboard migrations.
- [x] Make local package selection explicit for production-mode verification without pretending an unreleased package exists on Hex. Example dependency selection:

```elixir
if Mix.env() != :prod or System.get_env("PGFLOW_DEMO_LOCAL") == "1" do
  {:pgflow, path: ".."}
else
  {:pgflow, "~> 0.3.1"}
end
```

  Retain the production requirement until the synchronized library version is selected/released in an authorized release task. Mark deployment blocked if that requirement resolves to a package lacking the new migrations. A production build claiming to test alignment must prove it resolved the parent checkout or the eventual synchronized release. Existing demo-only Docker context cannot use `..`; document the repository-root build profile needed for local verification.
- [x] Align the demo's Elixir requirement with the library's existing minimum, parameterize dedicated test DB connection settings, and await successful FlowStarter module startup before sending demo work. Database fixtures must not start workers before migration completes.
- [x] Run both fresh migration replay and populated upgrade. Expect all original scenarios and existing dashboard routes to load. Update README to run committed migrations, not regenerate duplicate setup/definition migrations.

### Task D2: Build the executable scenario catalogue

**Depends on:** D1 and parent Tasks 7–9.

**Files:** Create all catalogue, runner, flow/job and scenario-test files in the file map; modify `demo/lib/pgflow_demo/application.ex` and existing `demo/lib/pgflow_demo/flows.ex` as needed to register definitions without duplicating registry ownership.

**Interfaces:** `PgflowDemo.Scenarios.list/0` returns finite scenario descriptors with string ID, title, feature tags, module, preset inputs and expected outcomes. `fetch/1` returns `{:ok, descriptor}` or `{:error, :unknown_scenario}`. `ScenarioRunner.start(id, preset)` returns `{:ok, run_id}` or a validation/library error; `load(run_id)` uses public typed PgFlow queries.

- [x] Add catalogue tests requiring every coverage row above to point to an existing executable scenario or concrete operational walkthrough. Add execution assertions for deterministic retry:

```elixir
assert {:ok, run_id} = ScenarioRunner.start("retry", "succeed_on_third_attempt")
# Wait using monitored execution / bounded database reconciliation.
# Then assert the stored task has attempts_count == 3 and the run completed.
```

  Implement the bounded reconciliation helper in `demo/test/support/scenario_case.ex`; it must raise on timeout and include last persisted status in the failure. Run `mix test test/pgflow_demo/scenarios_test.exs test/pgflow_demo/scenario_runner_test.exs` and verify expected missing-scenario failures.
- [x] Create flow modules using the library DSL, with no new compiler migrations for the new definitions. Example retry handler:

```elixir
fn input, ctx ->
  if ctx.attempt < input["succeed_on_attempt"] do
    {:error, "Demonstration retry"}
  else
    {:ok, %{"attempt" => ctx.attempt}}
  end
end
```

  Add fixed policy modules where modes are compile-time options, using finite explicit variants for `when_unmet`/`when_exhausted` rather than mutating a shared running definition from the UI. Place additional variants in individual files under `demo/lib/pgflow_demo/flows/policies/` and list them in the catalogue. Map/parallel examples use controlled local work, not external services. `RecordJob` returns its payload/attempt without sending mail.
- [x] Validate presets server-side with finite bounds for task counts, payload size, retry count and delay. Catalogue IDs/presets are string keys mapped to known modules. Keep arbitrary SQL, module names and flow deletion targets out of browser parameters.
- [ ] Implement recovery/drain walkthroughs in a test-bed-only module `demo/lib/pgflow_demo/scenario_controls.ex`. It validates a run belongs to the selected demo scenario before releasing/crashing its controlled handler or requesting a worker operation. Default runtime config disables destructive controls. Tests enable the profile explicitly and prove disabled/foreign-run requests fail. Release/drain/restart and ownership guards are verified; the requested controlled handler crash/recovery path is not implemented.
- [ ] Run real DB scenario tests for the full matrix. Assert persisted task/run outcomes and outputs, not only rendered labels. Cron schedule tests verify actual registration and invocation through controlled pg_cron fixtures without waiting for the wall-clock minute; verify the real scheduled tick in the D4 browser walkthrough.

### Task D3: Expose scenarios with durable run inspection

**Depends on:** D2.

**Files:** Create `demo/lib/pgflow_demo_web/live/scenarios_live.ex`, `demo/lib/pgflow_demo_web/components/scenario_source.ex`, `demo/test/pgflow_demo_web/live/scenarios_live_test.exs`; modify `demo/lib/pgflow_demo_web/router.ex`, `demo/lib/pgflow_demo_web/live/flow_demo_live.ex`, and `demo/lib/pgflow_demo_web/components/layouts.ex` for navigation only where practical.

**Interfaces:** Add `/scenarios` and `/scenarios/:scenario_id`; link them from the existing demo. Each scenario shows a short explanation, preset input, runnable action, source example, expected outcome, current run/task state, and a dashboard link. Tests use stable DOM IDs.

- [x] Add LiveView tests for catalogue discovery, selecting a preset, starting a run, validation errors and run inspection:

```elixir
{:ok, view, _html} = live(conn, "/scenarios/retry")
assert has_element?(view, "#scenario-input")
view |> element("#scenario-form") |> render_submit(%{"preset" => "succeed_on_third_attempt"})
assert has_element?(view, "#scenario-run")
assert has_element?(view, "#scenario-dashboard-link")
```

  Run `mix test test/pgflow_demo_web/live/scenarios_live_test.exs` and confirm missing route/control failures.
- [x] Implement the LiveView using `<Layouts.app>`, `to_form/2`, existing `<.input>` components, bounded streams for event/task lists, and public PgFlow LiveClient/query APIs. Persist the selected run in a URL parameter so reload and a second tab can reconstruct it. Do not use event order alone to derive terminal state.
- [x] Add tests switching scenarios while a previous run emits events: old run events must not overwrite the current scenario. On reconnect/load, fetch authoritative task/run state; display skipped/cancelled siblings, actual attempts, mixed-case slug/canonical queue, and JSON false/null distinctly. Avoid suggesting a cancelled SQL task guarantees the handler process had no remaining external effects.
- [x] Keep advanced operational controls visually separate and available only in the test-bed profile. Add accessible labels, disabled/busy states, and mobile layout. Source display must match the actual selected definition and must not invent await APIs or private per-step routes.
- [x] Run new and existing LiveView tests. Existing article/onboarding/cron experiences must remain navigable and preserve their outputs after scenario switching.

### Task D4: Make the demo a release acceptance gate

**Depends on:** D1–D3 and parent Task 10.

**Files:** Create `demo/docs/SCENARIOS.md`, `demo/lib/mix/tasks/pgflow_demo.verify_scenarios.ex`; modify `demo/README.md`, `.github/workflows/upstream-compatibility.yml`; add tests for the verification task in `demo/test/mix/tasks/pgflow_demo.verify_scenarios_test.exs`.

**Interfaces:** `mix pgflow_demo.verify_scenarios` runs deterministic scenarios in a dedicated demo test-bed database, reports each expected/actual outcome, and exits nonzero for any failure. External article/LLM integration is opt-in and reported separately. It never deletes unrelated run history or enables recurring jobs without restoring their prior state.

- [x] Add a test with an intentionally wrong scenario expectation and assert the verifier fails. Implement the task by enumerating the catalogue, starting each preset and comparing persisted outcome via the same runner used by the UI. Do not create a second scenario implementation for verification.
- [ ] Run from `demo`:

```sh
mix precommit
mix assets.build
MIX_ENV=test mix pgflow_demo.verify_scenarios
PGFLOW_DEMO_LOCAL=1 MIX_ENV=prod mix compile --warnings-as-errors
```

  Verify dependencies/config target the intended library and demo fixture before invoking commands. Production compile is a build check; it does not authorize starting a production application or applying migrations there.
- [ ] Start the dedicated demo server on an available port and exercise the browser: all catalogue routes; normal/empty maps; retry; skip/cancel; JSON false/null; immediate/delayed job; cron toggle and real tick; reload mid-run; scenario switching; dashboard navigation; test-bed worker drain/crash and subsequent recovery. Inspect browser errors, responsive layout and runtime logs. LLM integration runs only when explicitly configured and is never a required paid test.
- [ ] Test app restart with persisted queued/started work and verify recompiled/verified definitions, job classification, notification re-registration, and no duplicate cron schedules. Repeat with polling and notify configurations as separate application boots, not an unsupported runtime toggle.
- [x] Add demo verification to the parent CI acceptance workflow, with separate demo database and explicit startup readiness. Publish no deployment; attach commands, scenario results, browser findings and the tested upstream SHA to the release evidence. Parent Task 11 remains incomplete until these results pass.

## Acceptance and execution ledger

- [x] Existing demo upgrade and fresh replay pass without losing history or queues.
- [ ] Every supported feature family has an executable scenario or tested operational walkthrough.
- [x] Baseline scenario verification requires no network or credentials.
- [x] LiveView reconnect and scenario switching display database-authoritative results.
- [x] Operational fault controls are restricted to the explicit test-bed profile and demo-owned work.
- [ ] Demo precommit, assets, scenario runner, browser walkthrough and restart tests pass.
- [x] Production dependency/build context is documented and cannot silently validate an old Hex package.

Planning only as of 2026-09-14. Record generated migration filename, scenario additions, focused test results, browser evidence and remaining failures here during execution. No scenario or upgrade has been implemented by this plan-writing task.

Completion checkbox audit performed on 2026-09-14 against the final remediation reports and logs. See `.superpowers/sdd/plan-completion-audit.md` for the evidence map. The real cron tick, controlled crash/recovery browser case, notify-profile restart boot, and exact `mix precommit` invocation remain unverified.
