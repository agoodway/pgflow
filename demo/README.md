# PgFlow Demo

A Phoenix LiveView application that demonstrates PgFlow with a real-time article processing workflow.

```
fetch_article → convert_to_markdown → summarize        → publish
                                    ↘ extract_keywords ↗
```

## Setup

```bash
# 1. Start database (from root pgflow directory)
cd ..
docker compose up -d
cd demo

# 2. Install dependencies
mix deps.get

# 3. Create database and run committed migrations
mix ecto.create
mix ecto.migrate

# 4. Setup assets
mix assets.setup

# 5. Configure your LLM API key (see "LLM configuration" below)
cp .env.sample .env
$EDITOR .env

# 6. Run the server (or `pgflow start` from the repo root for hivemind + docker)
mix phx.server
```

The demo keeps its historical committed migrations for Postgres extensions, pgmq,
pgflow setup, flow/job compilation, and dashboard upgrades. Do **not** regenerate
duplicate setup or definition migrations on existing databases. The committed
`upgrade_pgflow_upstream_alignment` wrapper advances core V02, helpers V05, and
dashboard V04; `add_pgflow_helpers_v06` then adds stalled-task escalation and
the `pgflow.step_progress` view. `upgrade_pgflow_core_v03_helpers_v07` moves an
existing database to core V03 and helpers V07. Follow
[the stop-and-drain guide](../docs/UPGRADING_UPSTREAM_2026_10.md) before that
migration.

On fresh databases, `mix ecto.migrate` replays the full chain. The earliest
`setup_pgflow` migration calls a bare `up()`, so it installs the latest core and
helpers versions, and the later pinned wrappers (V02, V05, V06, V03/V07) are
then no-ops. On existing
installations, Ecto runs only the pending migrations.

- Demo app: http://localhost:4022
- PgFlow Dashboard: http://localhost:4022/pgflow

## Scenario verification (release gate)

Executable and check scenarios are documented in [docs/SCENARIOS.md](docs/SCENARIOS.md).
Baseline verification uses the same `ScenarioRunner` and `ScenarioChecks` as
the LiveView UI and requires no network or LLM credentials. Check scenarios
(`slug_rules`, `schema_contract`, `step_mode_guard`) cover the pgflow 0.17.2
slug rules, schema contract, and the Elixir refusal of step-mode flows:

```bash
MIX_ENV=test mix pgflow_demo.verify_scenarios
```

Article/LLM integration is opt-in (`--include-llm`) and never required for CI.

## Local production verification

The demo temporarily uses `{:pgflow, path: ".."}` in every Mix environment
while the 0.7.0 release is being tuned. Production-mode verification must run
from the demo directory with the parent checkout available:

```bash
MIX_ENV=prod mix compile --warnings-as-errors
```

Production refuses to boot unless its PgFlow dependency includes core V03 and
helpers V07. The demo-only Docker context contains no parent checkout, so its
image build cannot resolve the temporary path dependency. Include the parent
checkout in that context or return to a Hex dependency after publication.

## Release acceptance commands

From `demo/`:

```bash
mix precommit
mix assets.build
MIX_ENV=test mix pgflow_demo.verify_scenarios
MIX_ENV=prod mix compile --warnings-as-errors
```

## LLM configuration

The `summarize` and `extract_keywords` steps call **DeepSeek V3.2** on
**Fireworks AI** through ReqLLM by default.

1. Get a key at <https://fireworks.ai/account/api-keys> (looks like `fw_...`).
2. `cp .env.sample .env` and set `AI_API_KEY=fw_...`.

`.env` is gitignored. `runtime.exs` loads it via dotenvy.
