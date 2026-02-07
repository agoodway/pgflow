# PgFlow Demo

A Phoenix LiveView application that visualizes PgFlow workflow execution in real-time.

## Demo Flow: Article Processing

```
fetch_article → convert_to_markdown → summarize        → publish
                                    ↘ extract_keywords ↗
```

## Database (Docker)

The demo uses the same PostgreSQL container as the main pgflow library. Run these commands from the **root pgflow directory** (not the demo folder):

```bash
# Start the database
docker compose up -d

# Stop the database (preserves data)
docker compose down

# Stop and remove all data (full reset)
docker compose down -v
```

## Setup

```bash
# 1. Start database (from root pgflow directory)
cd ..
docker compose up -d
cd demo

# 2. Install dependencies
mix deps.get

# 3. Create the database
mix ecto.create

# 4. Generate PgFlow extensions migration (worker registration, flow queries)
mix pgflow.gen.extensions_migration

# 5. Generate PgFlow dashboard migration (dashboard views and functions)
mix pgflow_dashboard.gen.migration

# 6. Generate flow migration (compiles ArticleFlow definition to database)
mix pgflow.gen.flow PgflowDemo.Flows.ArticleFlow

# 7. Generate job migration (compiles ArticleFlowCleanup scheduled job to database)
mix pgflow.gen.job PgflowDemo.Jobs.ArticleFlowCleanup

# 8. Run all migrations
mix ecto.migrate

# 9. Setup assets
mix assets.setup

# 10. Set your OpenAI API key (required for LLM steps)
export OPENAI_API_KEY="sk-..."

# 11. Run the server
mix phx.server
```

Open http://localhost:4000

## Flow Compilation

PgFlow requires flows to be "compiled" into the database before workers can process them. This creates the flow record and PGMQ queue.

```bash
# Generate a migration for any flow module
mix pgflow.gen.flow MyApp.Flows.MyFlow

# Apply the migration
mix ecto.migrate
```

The generated migration will:
1. Create the flow record in `pgflow.flows`
2. Create the PGMQ queue `pgmq.q_<flow_slug>`
3. Register all steps with their dependencies in `pgflow.steps`

If you try to start a worker for a flow that hasn't been compiled, you'll get a helpful error message telling you exactly what command to run.

## AI Configuration

Set your OpenAI API key for the LLM steps:

```bash
export OPENAI_API_KEY="sk-..."
```
