# Brief: Durable External Waits and Upstream Alignment

> Revised 2026-10-31. The 2026-09-14 brief asked which abstraction to adopt.
> That choice is now made (see the decision record). This revision records the
> current state of both projects and the next step.

## Assignment

Plan a general-purpose durable external-wait capability for the Elixir PgFlow
implementation that stays aligned with the TypeScript/pgflow-dev project and
keeps a sound, portable PostgreSQL task protocol. This is a PgFlow capability,
not an Inbox feature, and must not be shaped around one application's workflow.

## Decision record

- 2026-08-28: [#660](https://github.com/pgflow-dev/pgflow/issues/660) opened,
  citing Elixir [PR #6](https://github.com/agoodway/pgflow/pull/6)
  (`await_signal` with park, requeue and handler replay).
- 2026-08-28: the maintainer
  [preferred an explicit manual task](https://github.com/pgflow-dev/pgflow/issues/660#issuecomment-5450794243):
  a handlerless DAG step that becomes ready, sends no PGMQ message, starts no
  worker, and is completed or failed by trusted server code. Ordinary
  downstream progression continues. No handler replay, and the wait stays
  visible in the DAG. The design is tracked in
  [#661](https://github.com/pgflow-dev/pgflow/issues/661).
- 2026-09-14: we agreed to align with the manual-task direction. The
  maintainer pointed to epic [#653](https://github.com/pgflow-dev/pgflow/issues/653)
  and #661 and asked for the concrete use case. **That question is still
  unanswered.**

**Decision:** the Elixir core primitive is the upstream manual task (#661).
Inline `await_signal` with handler replay (PR #6) is not pursued. Early payload
buffering, named signals and built-in deadlines are possible later layers on
top of manual tasks, not part of the first primitive.

## Current state (2026-10-31)

### Elixir PgFlow

- `main` is `3770b0d`, released as `v0.5.0`. It has no manual-task or
  external-wait API. `signal_strategy: :notify` only wakes queue workers.
- [PR #9](https://github.com/agoodway/pgflow/pull/9)
  (`feat/upstream-0-17-schema-parity`) ships as `0.6.0`: core V03 and helpers
  V07 give schema parity with upstream `pgflow@0.17.2` (`baa9e43f`), apart from
  documented Elixir overlays. The compatibility harness proves it (catalog diff,
  294 upstream pgTAP tests, real TypeScript workers including a mixed
  TypeScript and Elixir run). V03 adds `flows.queue_mode` (`flow` or `step`),
  per-step routes, `queue_name` snapshots on tasks, the 5-argument
  `start_tasks` and the 4-argument `ensure_flow_compiled`. Elixir workers refuse
  step-mode flows.
- `feat/worker-controls` ships next as `0.7.0`: per-worker
  `max_concurrency`/`batch_size` overrides and runtime `scale_worker/2`. No
  schema change.
- PR #6 (`feat/await-signals`) is open but untouched since 2026-08-27. It is
  based on `v0.3.1`, and its helpers "V05" collides with `main`'s V05 (8-column
  claim), V06 (stall escalation) and PR #9's V07. It is superseded by the
  decision above and should be closed with a link to #660 and #661.

### Upstream pgflow

- Released `0.17.1` (private step queues
  [#683](https://github.com/pgflow-dev/pgflow/pull/683), force-skip fix
  [#686](https://github.com/pgflow-dev/pgflow/pull/686), telemetry) and
  `0.17.2` (telemetry endpoint fix). `main` (`f67145b0`) adds
  [#708](https://github.com/pgflow-dev/pgflow/pull/708), a TypeScript-only
  falsy-output fix with no schema change.
- Queue identity and private per-step queues (#650, #651) have shipped. That is
  the foundation #661 requires, and Elixir has it through PR #9.
- #661 is still an open, deferred design spike with no comments or PRs since
  2026-09-12. Its first exit gate is "One real TypeScript use case and caller
  are documented."
- #660 is open at P3. [#665](https://github.com/pgflow-dev/pgflow/issues/665)
  (portable migrations and Node/Bun workers) is open.
- New work that may touch the task lifecycle: the Dirty Runs epic
  [#699](https://github.com/pgflow-dev/pgflow/issues/699) (#700 to #706) and
  the community-threads plan [#710](https://github.com/pgflow-dev/pgflow/issues/710).
- Neither manual tasks nor durable signals have shipped upstream.

## Alignment matrix

| Concern            | Upstream 0.17.2                                   | Upstream direction (#661)                                        | Elixir after PR #9      | Gap                                              |
| ------------------ | ------------------------------------------------- | ---------------------------------------------------------------- | ----------------------- | ------------------------------------------------ |
| Task identity      | `(run_id, step_slug, task_index)` + `queue_name`  | Manual task keeps the address, has no queue message              | Same as upstream        | None                                             |
| Execution mode     | `flows.queue_mode` is `flow` or `step`            | Must distinguish manual from queued work; no fake queue name     | Flow mode only          | Await upstream schema; bootstrap must allow it   |
| Completion         | Worker-owned `complete_task` / `fail_task`        | Separate trusted-server complete and fail entry points           | Worker-owned only       | Inherit from upstream                            |
| Claims, recovery   | Queue-based claims and stalled recovery           | Must exclude manual tasks                                        | Same as upstream        | Inherit from upstream                            |
| Authorization      | No external write boundary                        | Revoke from `PUBLIC`; explicit grants for trusted server roles   | App role                | Match upstream grants                            |
| Deadlines, buffers | None                                              | Out of scope; application-owned                                  | None                    | Consumers own deadlines and early events         |

## Next steps

1. Answer the maintainer on #660 with the concrete use case. Inbox Report
   Assist Photos: an Elixir flow fans out workfile photo classification, and a
   "photos classified" gate should wait without holding a worker slot until
   server code sees every expected classification run terminal, then complete
   the gate once. A persisted deadline yields a partial result.
2. Offer to build the #661 prototype upstream. Most of it is shared SQL
   (manual task state, trusted complete and fail entry points, claim and
   recovery exclusion). Once upstream lands it, Elixir imports it through the
   existing bundle importer as the next core version.
3. Fallback only if a consumer needs it before upstream: an Elixir helpers
   overlay that follows #661 semantics exactly, limited to one ordinary step
   (no map variant, no signal store), designed to be replaced by the upstream
   version.
4. Close PR #6 as superseded.

## Constraints

- A PgFlow capability, not an Inbox-specific API or a dependency workaround.
- No fake queue name and no weakened non-null queue constraints. If manual
  tasks need a distinct persisted execution mode, model and migrate it
  explicitly, following upstream.
- No Elixir process, Edge Function, database connection or worker slot held
  while an external result is pending.
- Completion and failure are a trusted-server boundary. Never expose them to
  browsers or unauthenticated SQL roles.
- PostgreSQL notifications may accelerate work but are never correctness
  evidence. Durable task state and atomic transitions are the source of truth.
- Evaluate task-address semantics, repeated waits, map task indices and fan-in
  before choosing a public contract.
- Do not merge PR #6 or pin a consumer to it.

## Open decisions

- Upstream-first prototype or Elixir overlay first, based on consumer timing.
- Elixir DSL naming, following whatever upstream picks (`queue: false` is a
  proposal, not a committed API).
- How consumers own deadlines and completions that arrive before the manual
  task exists.
- Map and array manual tasks stay out of the first slice.
