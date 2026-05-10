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

# 3. Generate migrations
#
#    `pgflow.setup --dashboard` writes a single consumer migration that
#    calls `PgFlow.Migration.up/0`, `PgFlow.HelpersMigration.up/0`, and
#    `PgFlowDashboard.Migration.up/0` — SQL is vendored inside pgflow.
#
#    On Postgres environments that already ship pgmq as an extension
#    (Supabase, the bundled atlas-postgres-pgflow test image), the
#    `CREATE EXTENSION pgmq` in this demo's `install_extensions.exs` is
#    enough. On plain Postgres, run `mix pgflow.gen.pgmq_migration` first
#    to install pgmq via SQL-only method.
mix pgflow.setup --dashboard
mix pgflow.gen.flow_migration PgflowDemo.Flows.ArticleFlow
mix pgflow.gen.job_migration PgflowDemo.Jobs.ArticleFlowCleanup

# 4. Create database and run migrations
mix ecto.create
mix ecto.migrate

# 5. Setup assets
mix assets.setup

# 6. Configure your LLM API key (see "LLM configuration" below)
cp .env.sample .env
$EDITOR .env

# 7. Run the server (or `pgflow start` from the repo root for hivemind + docker)
mix phx.server
```

- Demo app: http://localhost:4022
- PgFlow Dashboard: http://localhost:4022/pgflow

## LLM configuration

The `summarize` and `extract_keywords` steps call **DeepSeek V3.2** on
**Fireworks AI** through ReqLLM by default.

1. Get a key at <https://fireworks.ai/account/api-keys> (looks like `fw_...`).
2. `cp .env.sample .env` and set `AI_API_KEY=fw_...`.

`.env` is gitignored. `runtime.exs` loads it via dotenvy.
