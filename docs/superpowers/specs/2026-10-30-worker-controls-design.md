# Per-Worker Options and Runtime Worker Scaling

Date: 2026-10-30
Status: approved in conversation (Chase, Claude, Codex adversarial review)

## Why

PgFlow starts one `PgFlow.Worker.Server` per flow or job module, and every
worker takes `max_concurrency` and `batch_size` from the single supervisor
config (`PgFlow.WorkerSupervisor.start_worker_process/2`). A host cannot give
one heavy flow a smaller task budget than the rest. Appraisal Inbox needs this
for a chunked import flow whose `map` step fans out hundreds of database-heavy
tasks: the import worker must run 4 at a time while other workers keep their
default.

TypeScript pgflow already has the shape: concurrency is worker configuration
passed when a flow's worker starts (`EdgeWorker.start(MyFlow, { maxConcurrent: 5 })`,
default 10), never a Flow DSL option. The DSL holds only database-persisted
options (`maxAttempts`, `baseDelay`, `timeout`, `startDelay`, `if`, `ifNot`,
`whenUnmet`, `whenExhausted`).

Open-source Oban adds runtime control on top of static per-queue limits:
`Oban.scale_queue/2` changes a running queue's limit in memory,
`Oban.check_queue/2` reports it, scale-down lets running jobs finish, scale-up
dispatches immediately, and the change resets to configuration on restart.

## Goals

- Static per-worker overrides of `max_concurrency` and `batch_size`, set in the
  supervisor config and on `PgFlow.start_worker/2`.
- Runtime scaling of a running local worker (`PgFlow.scale_worker/2`) and
  inspection (`PgFlow.check_worker/1`), with Oban's open-source semantics.
- No change to PgFlow's database contract. TypeScript workers and flows keep
  working against the same database; the change is additive (0.5.0 to 0.6.0).

## Non-goals

- Persisted scaling (Oban Pro DynamicQueues). A host that wants a scaled value
  to survive restarts stores it itself and applies it at boot.
- Cross-node scaling, `node:`/`local_only` options, global limits, partitioned
  limits, rate limits.
- Pause/resume. Possible later with the same pattern.
- Per-run or per-step concurrency limits.

## Anti-goals

- Any change to `pgflow.*` schema, SQL functions, pgmq queues, or the
  serialized flow shape (`PgFlow.Flow.Shape`). A concurrency option inside
  `@flow`/`step` options would enter the DSL and risk the shape; it stays out.
- Scale-down killing or interrupting active tasks.

## Design

### Configuration

New supervisor option `worker_options`: a map from flow or job module to a
keyword list. Allowed keys are exactly `:max_concurrency` and `:batch_size`,
both positive integers. Every key in the map must be a module listed in
`:flows` or `:jobs`; anything else raises `ArgumentError` at validation.

```elixir
{PgFlow,
 repo: MyApp.Repo,
 max_concurrency: 8,
 jobs: [MyApp.Flows.Import],
 worker_options: %{MyApp.Flows.Import => [max_concurrency: 4, batch_size: 4]}}
```

`PgFlow.start_worker(module, max_concurrency: 4)` accepts the same keys
(alongside `:repo`). Effective options resolve as: global config, then
`worker_options[module]`, then `start_worker/2` opts.

The resolved overrides travel in the worker's child spec start arguments, so a
supervisor restart after a crash restarts with the configured values.

Calling `start_worker/2` for a module whose worker is already running returns
`{:ok, pid}` when the call passes no overrides or overrides equal to the
worker's configured values, and `{:error, {:conflicting_worker_options, pid}}`
otherwise. Concurrent first starts of the same module are serialized so only
one worker is created.

### Runtime scaling

`PgFlow.scale_worker(module, opts)`:

- `opts` takes `:max_concurrency` and/or `:batch_size` (at least one), positive
  integers; anything else raises `ArgumentError`.
- Synchronous: returns `{:ok, check_result}` only after the worker applied the
  change, so a following `check_worker/1` observes it.
- Local only. `{:error, :not_found}` when this node has no worker for the module.
- `{:error, :not_running}` when the worker is stopping or stopped.
- Scale-down only limits future claims; active tasks finish.
- Scale-up polls immediately under both `:polling` and `:notify`.
- Claims stay bounded by `min(batch_size, max_concurrency - active)`.
- Runtime-only: a restart returns to the configured values.

`PgFlow.check_worker(module)` returns `nil` when there is no local worker,
otherwise:

```elixir
%{
  module: module(),
  pid: pid(),
  lifecycle: :created | :running | :stopping | :stopped,
  max_concurrency: pos_integer(),
  batch_size: pos_integer(),
  configured_max_concurrency: pos_integer(),
  configured_batch_size: pos_integer(),
  active: non_neg_integer()
}
```

### Compatibility

No migration, no SQL, no shape change. A test proves a worker started with
overrides verifies against a flow compiled without them, and that the flow's
persisted `opt_*` columns are unchanged.

## Test coverage modeled on Oban (`test/oban_test.exs`)

| Oban test                                 | PgFlow test                                                     |
| ----------------------------------------- | --------------------------------------------------------------- |
| scale_queue "validating options"          | invalid scale options raise                                     |
| "scaling queues up" (visible via check)   | scale-up visible via check_worker and starts queued tasks now   |
| scale-down semantics (docs)               | scale-down finishes active tasks and limits new claims          |
| "trying to scale queues that don't exist" | scale/check of a module with no local worker                    |
| check_queue reports limit/paused/running  | check_worker reports effective, configured and active values    |
| (restart semantics)                       | a crashed worker restarts with configured, not scaled, values   |
