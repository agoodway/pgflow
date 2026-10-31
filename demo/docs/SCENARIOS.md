# Demo scenario catalogue

The demo test bed exposes a finite catalogue at `/scenarios`. Executable presets run through `PgflowDemo.ScenarioRunner` — the same module used by the LiveView UI and `mix pgflow_demo.verify_scenarios`. Check presets run through `PgflowDemo.ScenarioChecks`.

## Baseline verification (no network)

```bash
cd demo
MIX_ENV=test mix pgflow_demo.verify_scenarios
```

This enumerates every **executable** preset, starts it via `ScenarioRunner`, waits for a terminal persisted status, and compares the outcome to the catalogue `expected` map. Every **check** preset runs through `ScenarioChecks` and is compared the same way; checks are part of the gate and must pass. Walkthrough-only scenarios (article LLM, observability, startup compilation, multi-language, external waits) are reported but not required.

### Safety guarantees

- Requires `MIX_ENV=test` and uses `PGFLOW_DEMO_DB_NAME` (default `pgflow_demo_test`). Never point this command at a development or production database.
- Never deletes unrelated run history — only inserts new runs.
- Snapshots `cron.job` rows for `pgflow:%` before verification and restores any drift afterward.
- Recovery presets auto-release blocked handlers via `ScenarioControls` (test-bed only).
- `step_mode_guard` only writes the `ts_step_mode_shadow` flow, its steps, and its two step queues. It provisions them on the first run and verifies them afterwards; it never deletes data.

### Opt-in LLM integration

Article processing requires `AI_API_KEY`. It is never part of the baseline gate:

```bash
MIX_ENV=test mix pgflow_demo.verify_scenarios --include-llm
```

This flag only reports configuration status; it does not execute the LLM flow.
Manual integration verification remains on `/` (FlowDemoLive).

### Local recovery controls

Set `PGFLOW_DEMO_TESTBED=1` before starting a non-production demo. The recovery
Run button stays disabled otherwise. Drain is asynchronous so Release remains
usable; Restart starts the worker again after drain. Production always disables
the test-bed flag. Deliberate worker-crash/recovery demonstration is not implemented.

## Executable scenarios

| ID | Presets | Expected focus |
|----|---------|----------------|
| `parallel` | `default_fan_in` | completed |
| `map` | `three_items` | completed |
| `root_map` | `normal_list`, `empty_list`, `scalar_item` | completed |
| `onboarding` | `premium_ok`, `free_skip_cascade`, `fail_soft_email` | completed |
| `policy_if_met` | `default` | completed |
| `policy_if_not` | `default` | completed |
| `policy_when_unmet_skip` | `default` | completed (gated skipped) |
| `policy_when_unmet_skip_cascade` | `default` | completed (downstream skipped) |
| `policy_when_unmet_fail` | `default` | **failed** |
| `retry` | `succeed_on_third_attempt` | completed, 3 attempts |
| `exhaustion_fail` | `default` | failed |
| `exhaustion_skip` | `default` | completed |
| `exhaustion_skip_cascade` | `default` | completed |
| `timeout` | `fast_path`, `slow_path` | completed / failed with timed-out slow step |
| `delayed` | `one_second_delay` | completed |
| `json` | `false_and_null`, `scalar_value`, `list_values`, `object_values` | completed |
| `record_job` | `immediate_echo`, `delayed_enqueue` | completed |
| `scheduled` | `manual_tick` | completed |
| `recovery` | `blocked_handler` | completed (auto-released) |
| `queue_identity` | `mixed_case` | completed |

## Check scenarios

Check scenarios demonstrate guardrails and contracts rather than runs. Each one
names a module implementing `PgflowDemo.ScenarioChecks` (`run/1` takes the
preset input and returns a string-keyed result map). The preset's `expected` map
is compared key by key with that result, and keys not listed in `expected` are
informational. The `/scenarios/<id>` page shows a **Run check** form, a
passed/failed badge, one match/mismatch row per expected key, and the full
result.

| ID                | Presets                                                                                                                                  | Expected focus                                                                                                                                                       |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `slug_rules`      | `valid_flow_slug`, `reserved_run`, `double_underscore`, `leading_underscore`, `trailing_underscore`, `flow_queue_too_long`, `mixed_case` | `PgFlow.Slug` rule, `pgflow.is_valid_slug`, and the 47-character flow queue cap (`PgFlow.Slug.validate_queue_name` and `pgmq.validate_queue_name`) agree             |
| `schema_contract` | `installed_demo_db`                                                                                                                      | `compatibility_report/0` reports 0.17.2, core V03, helpers V07; `SchemaCheck.run/1` is ok; slug audit has no findings; live Elixir workers carry `elixir-<vsn>`      |
| `step_mode_guard` | `typescript_owned_flow`                                                                                                                  | `ts_step_mode_shadow` exists in step mode; `Bootstrap.prepare/2` returns `{:error, {:unsupported_queue_mode, slug, "step"}}`; flow, step, and dep rows are unchanged |

`slug_rules` presets use the slugs `order_pipeline`, `run`, `a__b`, `_leading`,
`trailing_`, `quarterly_portfolio_rebalancing_report_flow_v001` (48 characters:
a valid step slug whose flow queue name exceeds the cap), and `OrderFlow`
(valid; queue name `orderflow`).

`schema_contract` runs `PgFlow.Upgrade.SlugAudit.run/1`. Inside the Ecto SQL
sandbox, `SET TRANSACTION READ ONLY` fails with `25001` because the sandbox
transaction has already run statements; the audit then runs the same read-only
`SELECT` in the caller's transaction.

`step_mode_guard` stands in for a TypeScript worker using per-step queues. It
calls `pgflow.ensure_flow_compiled(slug, shape, 'step', route_map)` with the
shape of `PgflowDemo.Flows.TsStepModeShadowFlow` and a route map of
`ts_step_mode_shadow__fetch_order` and `ts_step_mode_shadow__notify`. That
module is not in the `PgFlow.Supervisor` flow list, so no worker ever serves
it. The check then runs `PgFlow.Worker.Bootstrap.prepare/2` for the module and
confirms the refusal, the unchanged definition rows, and that no
`elixir:PgflowDemo.Flows.TsStepModeShadowFlow` worker function was tracked.

## Walkthrough scenarios

| ID | Notes |
|----|-------|
| `article` | LLM/network integration via FlowDemoLive — opt-in only |
| `observability` | PgFlow dashboard at `/pgflow` |
| `startup_compilation` | FlowStarter compiles definitions at boot |
| `multi_language` | Parent upstream interoperability report |
| `external_waits` | Future work |

## Browser / LiveView coverage

`test/pgflow_demo_web/live/scenarios_live_test.exs` exercises catalogue routes, preset submission, URL `?run=` reload, scenario switching (foreign pubsub ignored), JSON false/null display, retry attempts, skipped steps, queue identity, test-bed controls visibility, article/onboarding navigation, and check result rendering for all three check scenarios.

`test/pgflow_demo/scenario_checks_test.exs` unit-tests the check modules inside a private SQL sandbox, including first-run provisioning and repeat-run verification for `step_mode_guard`.

## Signal strategy (polling vs notify)

`MIX_ENV=test` defaults to polling. Set `PGFLOW_DEMO_SIGNAL=notify` before a fresh
test boot to select notify. Actual notify acceptance requires pgmq with
`enable_notify_insert` support; the local pgmq 1.5.1 profile cannot prove it.

## App restart acceptance

`test/pgflow_demo/scenario_runner_test.exs` verifies that after draining and restarting a worker, a delayed-enqueued job still completes and pgflow cron rows are unchanged.

## Release gate commands

From `demo/`:

```bash
mix precommit
mix assets.build
MIX_ENV=test mix pgflow_demo.verify_scenarios
MIX_ENV=prod mix compile --warnings-as-errors
```

Production compile is a build check only — it does not start the app or run migrations in production.

## Release evidence

| Pin | SHA |
|-----|-----|
| Elixir baseline / worktree | `9dbaaafdbf9ef5fcdfb2c10df8edea3577b07846` |
| Upstream compatibility pin | `baa9e43f5f9122133deacbc3e8ffc80e284b4f50` |

The workflow is configured to verify the demo on `pgflow_demo_test`; a passing
GitHub run has not yet been observed. Each preset checks persisted step statuses;
deterministic map, fan-in, JSON, recovery, and queue-identity outputs have additional
value assertions. The historical 28/28 report checked mostly run status and is not
evidence of these stronger checks.
