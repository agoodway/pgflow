# PgFlow Demo

A Phoenix LiveView application that visualizes PgFlow workflow execution in real-time.

## Demo Flow: Article Processing

```
fetch_article → convert_to_markdown → summarize        → publish
                                    ↘ extract_keywords ↗
```

## Setup

```bash
# 1. Start database (from root pgflow directory, uses same docker-compose as library)
docker compose up -d

# 2. Install dependencies
cd demo
mix deps.get

# 3. Setup database and run migrations
mix ecto.setup

# 4. Generate flow migration (compiles flow definition to database)
mix pgflow.gen.flow PgflowDemo.Flows.ArticleFlow

# 5. Run the flow migration
mix ecto.migrate

# 6. Setup assets
mix assets.setup

# 7. Set your OpenAI API key (required for LLM steps)
export OPENAI_API_KEY="sk-..."

# 8. Run the server
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
