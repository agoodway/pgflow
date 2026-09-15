# Brief: Durable External Waits and Upstream Alignment

## Assignment

Investigate and plan a general-purpose durable external-wait capability for
the Elixir PgFlow implementation. This is not an Inbox feature and must not be
shaped around one application's photo-classification workflow. The objective is
to keep the Elixir implementation aligned, where appropriate, with the
TypeScript/pgflow-dev project while preserving a sound, portable PostgreSQL
task protocol.

This assignment is research and planning only. Do not change code, create a
branch, commit, publish, or merge anything. Inspect source, Git history,
issues, pull requests, schemas, and tests read-only. Report exact evidence and
separate confirmed facts from proposed design.

## Current State Established So Far

### Elixir PgFlow

- Local checkout: `/Users/chasepursley/Development/os/pgflow`.
- Current `main`: `9dbaaafdbf9ef5fcdfb2c10df8edea3577b07846`, released as
  `v0.3.4`.
- Current public `v0.3.4` has no durable external-signal or manual-task API.
  Its `signal_strategy: :notify` wakes queue workers; it is not a mechanism to
  suspend a task for an external result.
- PR [#6](https://github.com/agoodway/pgflow/pull/6), branch
  `feat/await-signals`, is an existing Elixir implementation of durable
  park-and-requeue waiting. At the last check it was open, conflicted/dirty,
  based on `v0.3.1`, 17 commits ahead and 3 commits behind current `main`, and
  its `mix.exs` still declared `0.3.1`. Do not assume it can be merged or
  released without reconciliation.
- PR #6 adds helpers schema V05 and
  `PgFlow.Context.await_signal/2` / `PgFlow.signal/3,4`. With
  `wait_timeout: 0`, `await_signal` durably parks a task, archives its PGMQ
  message, marks it waiting, and releases the worker slot. A matching signal
  requeues it. It provides early payload buffering, duplicate handling and a
  deadline/recovery path.
- Its important semantic cost is handler replay: the resumed task runs its
  handler from the beginning. Effects before the await must be idempotent. V1
  supports one await point per task. Its signal identity is the task address
  `(run_id, step_slug, task_index)`; payloads are JSON maps/lists.

Read the PR's current code and its full discussion before relying on this
summary. Key starting points:

- `lib/pgflow/context.ex`
- `lib/pgflow/client.ex`
- `lib/pgflow/worker/server.ex`
- `lib/pgflow/worker/waiting_task_recovery.ex`
- `priv/pgflow_helpers/sql/versions/v05/`
- PR #6's migration and compatibility notes

### TypeScript / upstream PgFlow

- Upstream checkout/repository: `pgflow-dev/pgflow`, current `main` last
  verified at `94490709f79ebf366141dd925b047f0c1013e759` (September 13, 2026).
- No equivalent durable external-wait capability has landed upstream. Recent
  merges include queue identity (#679), worker-startup test synchronization
  (#680), test lifecycle (#675), and startup-only compilation (#672), but not
  queue-less manual tasks or durable external signals.
- Upstream issue [#660](https://github.com/pgflow-dev/pgflow/issues/660) is an
  open, priority-P3 design spike. It identifies the necessary decisions:
  durable task identity, worker release, early/duplicate delivery, deadlines,
  terminal-state races, authorization, payload redaction, and an atomic task
  lifecycle. It explicitly says not to port Elixir PR #6 wholesale.
- In [the maintainer response on #660](https://github.com/pgflow-dev/pgflow/issues/660#issuecomment-5450794243), the preferred direction is not inline
  handler suspension. It is an explicit handlerless/manual DAG task: a task
  becomes ready, but no PGMQ message is sent and no worker runs. Trusted
  server-side code later completes or fails that task; ordinary downstream
  progression then continues. This avoids handler replay and leaves the wait
  visible in the DAG.
- Upstream issue [#661](https://github.com/pgflow-dev/pgflow/issues/661) is the
  detailed manual-task design, not an implementation. It proposes, but does
  not commit to, `queue: false` and `typeHint<T>()`. The task has a real address
  `(run_id, step_slug, task_index)`, no handler, no queue message, and durable
  state until trusted completion/failure or terminalization. It requires
  idempotent/race-safe completion, worker/recovery exclusion, historical
  execution-mode semantics, and server-only authorization. It intentionally
  leaves early payload buffering, a signal store, and built-in deadline
  recovery out of its first prototype.
- #661 had no comments or implementation PR at the last check. Its design is
  upstream intent, not a released contract.
- Upstream issue [#665](https://github.com/pgflow-dev/pgflow/issues/665) is a
  separate open portability proposal: a single pgflow-owned portable PostgreSQL
  migration history and Node/Bun process workers, with Supabase Realtime and
  Edge Function wakeups isolated as a repeatable Supabase integration. It
  explicitly excludes a non-Supabase browser client, WebSocket/SSE/LISTEN-NOTIFY
  gateways, and live browser updates. Treat it as an architectural constraint
  on any future wait design: durable lifecycle and worker semantics belong in
  the portable core; browser notification remains a platform/application
  integration. It is not an implementation of manual tasks or signals.

The upstream TypeScript client method `waitForStatus` only observes a run from
the caller; it is not a durable task suspension mechanism.

## Investigation Questions

Answer these from current source and GitHub history, not this brief alone.

1. What work already exists in either project?
   - Re-check PR #6's exact base/head, conflict state, commit list, migrations,
     test coverage, review history, and compatibility gaps relative to current
     Elixir `main`.
   - Search Elixir branches, open/closed PRs, issues, commits, TODOs, schemas,
     and docs for related waiting, signal, manual-completion, PGMQ, task-state,
     or recovery work.
   - Read the complete #660/#661 upstream threads and linked work. Search
     recent upstream merged and open PRs/branches for manual tasks, external
     completion, signals, waiting states, execution modes, queue-less tasks,
     worker lifecycle, or task-address changes.

2. What is the actual shared data-model baseline today?
   - Compare the current Elixir SQL schema/functions/helpers with upstream
     TypeScript core's current schema/functions, especially task identity,
     status constraints, queue identity, PGMQ dispatch, ready-step creation,
     worker claims, completion/failure, stalled recovery, terminal cleanup, and
     versioned migrations.
   - Include #665's proposed portable-core/Supabase-integration split in the
     comparison. Identify whether an external-wait design depends on any
     Supabase-only facility; it must not if Elixir is to remain generally
     portable.
   - Identify existing incompatibilities before adding a feature. Do not assume
     the implementations are still schema-identical merely because the README
     says they are compatible.
   - State whether upstream has a working branch/data model that Elixir can
     align with now, or only a design direction.

3. Which general abstraction should govern a future Elixir implementation?
   Compare at least these two choices:

   | Option | Core behavior | Principal tradeoff |
   |--------|---------------|--------------------|
   | Inline await/signal | Handler parks, signal requeues, handler restarts | Built-in buffering/deadlines possible; replay requires idempotent earlier effects and introduces waiting/signal lifecycle complexity |
   | Explicit manual task | Ready DAG task has no queue/handler; trusted code completes/fails it | Visible DAG and no replay; early buffering and deadline/fan-in layers must be designed separately |

   Recommend one as the core primitive and justify it for PgFlow generally,
   including multi-language workers and portable PostgreSQL—not for a single
   Inbox feature. It is acceptable to recommend an intentionally small manual
   task primitive first, with signals/deadlines as future layers, if that is
   what current upstream direction supports.

4. If the recommendation is adopted, plan a reconciled Elixir implementation.
   The plan must include:

   - a public API/DSL proposal clearly labeled proposed until approved;
   - persisted task identity and historical execution-mode behavior;
   - schema/helper versioning, upgrade/rollback safety, and startup schema
     compatibility;
   - atomic completion/failure transitions, idempotency, conflicting payloads,
     terminal-run races, and downstream progression exactly once;
   - worker claim, retry, stalled recovery, queue archival/pruning, and terminal
     cleanup exclusion rules;
   - authorization/grant model and payload/log redaction boundary;
   - test matrix covering direct tasks, one supported map/array position if
     chosen, early/duplicate completion, timeout policy, restart/recovery,
     concurrency, and cross-language/schema compatibility;
   - migration and release sequence that keeps Elixir aligned with the current
     upstream data model without pinning consumers to a stale feature branch.

## Constraints

- This must be a PgFlow capability, not an Inbox-specific API or a dependency
  upgrade workaround.
- Do not use a fake queue name or silently weaken non-null queue constraints.
  If manual tasks need a distinct persisted execution mode, model and migrate it
  explicitly.
- Do not keep an Elixir process, Edge Function, database connection, or worker
  slot alive while an external result is pending.
- Do not expose arbitrary task completion to browsers or unauthenticated SQL
  roles. Completion/failure is a trusted-server boundary.
- Do not treat PostgreSQL notifications as correctness. They may accelerate
  workers/reconciliation, but durable task state and atomic transitions must
  remain the source of truth.
- Do not assume one external event per task: evaluate task-address semantics,
  repeated waits, map task indices, named/correlated signals, and fan-in before
  choosing a public contract.
- Do not merge PR #6, pin to it, or modify either repository under this brief.

## Deliverable

Return a concise design/research report with:

1. Confirmed current state in both repositories and a link/path for each key
   claim.
2. An upstream-alignment matrix: present schema/lifecycle, planned/upstream
   direction, and Elixir gap.
3. Recommendation: adopt, revise, or stop; identify the smallest core
   prototype and why.
4. A sequenced implementation plan for a new, current-main-based Elixir branch
   (not a merge of stale PR #6), including migrations and verification gates.
5. Explicit open decisions that require maintainer approval before code starts.

Do not claim that upstream has shipped manual tasks or durable signals unless a
current merged implementation proves it.
