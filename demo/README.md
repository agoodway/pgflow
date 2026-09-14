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
duplicate setup or definition migrations on existing databases; apply new
upstream alignment work through the committed wrapper migration
`upgrade_pgflow_upstream_alignment`.

On fresh databases, `mix ecto.migrate` replays the full chain including that
wrapper. On existing installations, only the new wrapper migration runs.

- Demo app: http://localhost:4022
- PgFlow Dashboard: http://localhost:4022/pgflow

## Scenario verification (release gate)

Executable scenarios are documented in [docs/SCENARIOS.md](docs/SCENARIOS.md).
Baseline verification uses the same `ScenarioRunner` as the LiveView UI and
requires no network or LLM credentials:

```bash
MIX_ENV=test mix pgflow_demo.verify_scenarios
```

Article/LLM integration is opt-in (`--include-llm`) and never required for CI.

## Local production verification

The demo-only Docker build context cannot resolve `{:pgflow, path: ".."}`.
Production-mode verification against the parent checkout must run from the
demo directory:

```bash
PGFLOW_DEMO_LOCAL=1 MIX_ENV=prod mix compile --warnings-as-errors
```

Production refuses to boot with a PgFlow dependency whose bundled core version is
below 2. The demo-only Docker context currently resolves the published Hex
dependency, so it cannot serve the synchronized scenarios until a matching release
is published. `PGFLOW_DEMO_LOCAL=1` selects the parent checkout for local builds.

## Release acceptance commands

From `demo/`:

```bash
mix precommit
mix assets.build
MIX_ENV=test mix pgflow_demo.verify_scenarios
PGFLOW_DEMO_LOCAL=1 MIX_ENV=prod mix compile --warnings-as-errors
```

## LLM configuration

The `summarize` and `extract_keywords` steps call **DeepSeek V3.2** on
**Fireworks AI** through ReqLLM by default.

1. Get a key at <https://fireworks.ai/account/api-keys> (looks like `fw_...`).
2. `cp .env.sample .env` and set `AI_API_KEY=fw_...`.

`.env` is gitignored. `runtime.exs` loads it via dotenvy.
