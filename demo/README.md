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
mix pgflow.gen.extensions_migration
mix pgflow_dashboard.gen.migration
mix pgflow.gen.flow PgflowDemo.Flows.ArticleFlow
mix pgflow.gen.job PgflowDemo.Jobs.ArticleFlowCleanup

# 4. Create database and run migrations
mix ecto.create
mix ecto.migrate

# 5. Setup assets
mix assets.setup

# 6. Set your OpenAI API key (required for LLM steps)
export OPENAI_API_KEY="sk-..."

# 7. Run the server
mix phx.server
```

- Demo app: http://localhost:4000
- PgFlow Dashboard: http://localhost:4000/pgflow
