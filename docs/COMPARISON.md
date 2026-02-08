# Comparison with Alternatives

## Elixir Workflow Engines

| Aspect             | [PgFlow]                    | [Oban]                      | [Oban Pro Workflow]         | [Broadway]                  | [Gust]                      | [Handoff]                   | [Reactor]                   | [FlowStone]                 | [Durable]                   | [Journey]                     |
| ------------------ | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- | ----------------------------- |
| **License**        | Open source                 | Open source                 | Paid                        | Open source                 | Open source                 | Open source                 | Open source                 | Open source                 | Open source                 | Open source                   |
| **Focus**          | Cross-language workflow DAGs, jobs and cron | Background jobs with cron   | DAG workflows for Oban      | Kafka/SQS data pipelines    | Airflow-like DAGs with UI   | Distributed cluster DAGs    | Saga orchestration+rollback | Asset-first ETL pipelines   | Temporal-style workflows    | Durable graph workflows       |
| **Coordination**   | Database (pgmq)             | Database (Oban)             | Database (Oban)             | In-memory (GenStage)        | Application (Elixir)        | Erlang cluster              | In-process                  | Database (Oban)             | Database (PostgreSQL)       | Database (PostgreSQL)         |
| **Signal Strategy**| Polling + LISTEN/NOTIFY     | Polling + NOTIFY            | Polling + NOTIFY            | Demand-based (GenStage)     | Manual poll                 | Distributed messages        | In-process                  | Oban-based                  | Polling                     | Polling                       |
| **Dependencies**   | First-class `depends_on`    | Manual enqueue              | First-class `deps`          | Pipeline stages             | `downstream` option         | Explicit `args` refs        | Spark DSL `argument`        | First-class `depends_on`    | Pipeline (sequential)       | Explicit list in `compute`    |
| **Fan-out/Fan-in** | Built-in `map` steps        | Manual                      | Built-in patterns           | Partitioned batches         | Manual task chains          | Manual DAG build            | Manual composition          | Partition-based             | ForEach with concurrency    | Manual composition            |
| **State Storage**  | PostgreSQL (durable)        | PostgreSQL (durable)        | PostgreSQL (durable)        | In-memory                   | PostgreSQL                  | In-memory                   | In-memory                   | PG/S3/Parquet               | PostgreSQL (durable)        | PostgreSQL (durable)          |
| **Cross-platform** | Yes (TS + Elixir)           | Elixir only                 | Elixir only                 | Elixir only                 | Elixir only                 | Elixir only                 | Elixir only                 | Elixir only                 | Elixir only                 | Elixir only                   |
| **Compensation**   | Retry with backoff          | Retry with backoff          | Retry + dep options         | N/A                         | Retry                       | Max retries                 | Full saga undo              | Retry (via Oban)            | Saga rollback + retry       | Retry with recovery           |
| **Scheduling**     | Built-in cron DSL (pg_cron) | Built-in Oban.Cron          | Built-in Oban.Cron          | N/A                         | Built-in cron               | N/A                         | N/A                         | Via Oban                    | Built-in cron               | Built-in tick nodes           |
| **Web UI**         | Optional LiveView dashboard | Oban.Web (paid)             | Oban.Web (paid)             | N/A                         | Included                    | N/A                         | N/A                         | LiveView dashboard          | N/A                         | CLI introspection + analytics |
| **Resource-aware** | No                          | No                          | No                          | Demand-based                | No                          | Yes (cost maps)             | No                          | No                          | No                          | No                            |
| **Dynamic steps**  | No                          | N/A                         | Yes (grafting)              | N/A                         | No                          | No                          | Yes (runtime)               | No                          | Yes (branching)             | Yes (conditional logic)       |

[PgFlow]: https://github.com/pgflow-dev/pgflow
[Oban]: https://github.com/oban-bg/oban
[Oban Pro Workflow]: https://hexdocs.pm/oban
[Broadway]: https://github.com/dashbitco/broadway
[Gust]: https://github.com/marciok/gust
[Handoff]: https://github.com/polvalente/handoff
[Reactor]: https://github.com/ash-project/reactor
[FlowStone]: https://github.com/nshkrdotcom/flowstone
[Durable]: https://github.com/wavezync/durable
[Journey]: https://github.com/shipworthy/journey

## Other Workflow Engines

| Aspect             | [PgFlow]                    | [Temporal]                  | [Inngest]                   | [DBOS]                      | [Trigger.dev]               | [Vercel Workflows]          |
| ------------------ | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- | --------------------------- |
| **License**        | Open source                 | OSS + Cloud                 | OSS + Cloud                 | OSS + Cloud                 | OSS + Cloud                 | Paid hosted                 |
| **Focus**          | Cross-language workflow DAGs, jobs and cron  | Durable execution platform  | Event-driven step functions | Lightweight PG workflows    | Durable serverless tasks    | AI agent workflows          |
| **Coordination**   | Database (pgmq)             | Temporal Service            | Inngest engine              | PostgreSQL checkpoints      | Durable containers          | Vercel queues               |
| **Signal Strategy**| Polling + LISTEN/NOTIFY     | Long polling                | Webhook / polling           | Event-driven                | Webhook                     | Event-driven                |
| **Dependencies**   | First-class `depends_on`    | Sequential in code          | Step functions              | Decorators (`@step`)        | `triggerAndWait`            | Step isolation              |
| **Fan-out/Fan-in** | Built-in `map` steps        | Parallel activities         | `Promise.all()` steps       | DAG `depends_on`            | `batchTriggerAndWait`       | Parallel steps              |
| **State Storage**  | PostgreSQL (durable)        | Event History               | Managed persistence         | PostgreSQL checkpoints      | Container state             | Event log + replay          |
| **Cross-platform** | Yes (TS + Elixir)           | Go, Java, TS, Python        | TS, Python, Go              | TS, Python                  | TypeScript                  | TypeScript                  |
| **Compensation**   | Retry with backoff          | Full saga rollback          | Auto-retry + backoff        | Auto-retry + recovery       | Auto-retry                  | Deterministic replay        |
| **Scheduling**     | Built-in cron DSL (pg_cron) | Built-in timers + cron      | Built-in schedules          | Cron via `Schedule`         | Built-in queueing           | Sleep (min to months)       |
| **Web UI**         | Optional LiveView dashboard | Temporal Web UI             | Included dashboard          | Included dashboard          | Included dashboard          | Vercel dashboard            |
| **Resource-aware** | No                          | Worker scaling              | Serverless                  | No                          | Serverless                  | Serverless                  |
| **Dynamic steps**  | No                          | Yes (signals/queries)       | Yes (branching)             | Yes (decorators)            | Yes                         | Yes (hooks)                 |

[Temporal]: https://github.com/temporalio/temporal
[Inngest]: https://github.com/inngest/inngest
[DBOS]: https://github.com/dbos-inc/dbos-transact-ts
[Trigger.dev]: https://github.com/triggerdotdev/trigger.dev
[Vercel Workflows]: https://vercel.com/docs/workflow
