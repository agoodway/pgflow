# Per-Worker Options and Runtime Worker Scaling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a host give one flow's worker its own `max_concurrency`/`batch_size`, and change a running worker's limits at runtime the way open-source Oban's `scale_queue`/`check_queue` do.

**Architecture:** A new `PgFlow.WorkerOptions` module owns the allowlist, validation and resolution (global config, then `worker_options[module]`, then `start_worker/2` opts). `PgFlow.WorkerSupervisor` resolves options once per start and carries them in the child spec's start args so crash restarts keep them. `PgFlow.Worker.Server` gains two calls, `{:scale, opts}` and `:check`, and remembers its configured values so a restart resets runtime scaling. Nothing touches the database contract.

**Tech Stack:** Elixir, OTP (DynamicSupervisor, GenServer), NimbleOptions, ExUnit integration tests against the pgflow test database.

**Spec:** `docs/superpowers/specs/2026-10-30-worker-controls-design.md`

## Global Constraints

- No change to `pgflow.*` schema, SQL functions, pgmq queues, migrations or `PgFlow.Flow.Shape` serialization. TypeScript pgflow compatibility is unaffected.
- Allowed per-worker keys are exactly `:max_concurrency` and `:batch_size`, both `:pos_integer`.
- Effective option precedence: global supervisor config < `worker_options[module]` < `start_worker/2` opts.
- Runtime scaling is in memory and local to this node; a worker restart returns to its configured values.
- Scale-down never interrupts active tasks.
- Version becomes `0.6.0` (additive; no upgrade migration).
- Do not run `git commit`, `git push` or `mix hex.publish`. Leave changes in the working tree and suggest a message at each task end; Chase commits.
- Read `PgFlow.WorkerSupervisor` config with `Keyword.get(config, :worker_options, %{})`: tests start `WorkerSupervisor` with unvalidated keyword configs that lack the key.
- Integration tests that start `WorkerSupervisor` snapshot both `{PgFlow, :repo}` and `{PgFlow, :config}` persistent terms in setup and restore them (or erase when previously absent) in `on_exit`; `WorkerSupervisor.init/1` writes both.
- Concurrency tests use message-gated handlers, never sleeps or sampling: a fixture step sends `{:task_started, self(), input["n"]}` to the test pid (read from `:persistent_term.get({PgFlow.Test.GatedFlow, :test_pid})`) and blocks in `receive do :release -> :ok end`; the test counts `:task_started` messages and releases tasks with `send(task_pid, :release)`. Assert every run completes with status `"completed"`.

## Idiomatic Code Requirement

Before writing code in any task, load and follow `elixir-phoenix-guide:elixir-essentials`, `elixir-phoenix-guide:otp-essentials` (GenServer public-API wrappers, `@impl true`, bounded calls, supervision owns lifecycle) and `elixir-phoenix-guide:testing-essentials`. Match this repo's existing style (`lib/pgflow/worker_supervisor.ex`, `lib/pgflow/worker/server.ex`): typespecs and `@doc` on public functions, NimbleOptions for option schemas, no new process registries. Each task's review checks the diff against these skills, and `mix quality` must pass with no new Credo suppressions.

## Review Focus

- Scaling `max_concurrency` from 3 to 1 while 3 tasks run: all 3 finish, then at most 1 runs at a time. (Task 3 test.)
- Scale-up while the worker idles in `:notify` mode with a long fallback interval: queued tasks start within one poll, not at the next fallback. (Task 3 test.)
- A worker killed after being scaled restarts with its configured value, not the scaled one. (Task 4 test.)
- `worker_options` naming a module that is in neither `:flows` nor `:jobs` (a typo) fails at boot with `ArgumentError`. (Task 1 test.)
- `scale_worker` on a worker that is draining after `stop_worker` returns `{:error, :not_running}` and does not change its limits. (Task 3 test.)

---

### Task 1: `PgFlow.WorkerOptions` and the `worker_options` config key

**Files:**
- Create: `lib/pgflow/worker_options.ex`
- Modify: `lib/pgflow/config.ex` (schema entry, moduledoc option list, cross-check in `validate!/1`)
- Test: `test/pgflow/worker_options_test.exs`, `test/pgflow/config_test.exs`

**Interfaces:**
- Produces:
  - `PgFlow.WorkerOptions.keys() :: [:max_concurrency | :batch_size]`
  - `PgFlow.WorkerOptions.validate!(keyword()) :: keyword()`: allowlist + positive integers; raises `ArgumentError` naming the bad key or value. Empty list is valid here.
  - `PgFlow.WorkerOptions.validate_scale!(keyword()) :: keyword()`: like `validate!/1` but raises on an empty list ("scale_worker requires :max_concurrency or :batch_size").
  - `PgFlow.WorkerOptions.split(keyword()) :: {worker_opts :: keyword(), rest :: keyword()}`: separates allowlisted keys from others (e.g. `:repo`).
  - `PgFlow.WorkerOptions.resolve(global :: keyword(), module(), overrides :: keyword()) :: %{max_concurrency: pos_integer(), batch_size: pos_integer()}`: applies the precedence; reads `Keyword.get(global, :worker_options, %{})`.
  - Config key `worker_options: %{module() => keyword()}`, default `%{}`.

- [ ] **Step 1: Write the failing tests**

`test/pgflow/worker_options_test.exs` (`async: true`):
- `"validate!/1 accepts max_concurrency and batch_size"`: `assert WorkerOptions.validate!(max_concurrency: 4, batch_size: 2) == [max_concurrency: 4, batch_size: 2]`.
- `"validate!/1 rejects unknown keys, zero and non-integers"`: `assert_raise ArgumentError, ~r/:timeout/` for `[timeout: 5]`; same for `[max_concurrency: 0]` (`~r/max_concurrency/`) and `[batch_size: "4"]`.
- `"validate_scale!/1 requires at least one option"`: `assert_raise ArgumentError, ~r/requires :max_concurrency or :batch_size/, fn -> WorkerOptions.validate_scale!([]) end`.
- `"resolve/3 applies global, then worker_options, then overrides"`: with `global = [max_concurrency: 8, batch_size: 8, worker_options: %{Foo => [max_concurrency: 4]}]`: `resolve(global, Foo, [])` is `%{max_concurrency: 4, batch_size: 8}`; `resolve(global, Foo, batch_size: 2)` is `%{max_concurrency: 4, batch_size: 2}`; `resolve(global, Bar, [])` is `%{max_concurrency: 8, batch_size: 8}`; `resolve([max_concurrency: 8, batch_size: 8], Foo, [])` (no key) is `%{max_concurrency: 8, batch_size: 8}`.
- `"split/1 separates worker keys from other start options"`: `split(repo: R, max_concurrency: 4) == {[max_concurrency: 4], [repo: R]}`.

`test/pgflow/config_test.exs`, new `describe "worker_options"` (use the existing test repo and test flow/job modules from `test/support/test_flows.ex`, e.g. `PgFlow.TestFlows.SimpleFlow`, `PgFlow.TestJobs.SimpleJob`):
- `"defaults to an empty map"`.
- `"accepts options for listed flows and jobs"`: `validate!(repo: TestRepo, flows: [SimpleFlow], jobs: [SimpleJob], worker_options: %{SimpleFlow => [max_concurrency: 2], SimpleJob => [batch_size: 3]})` returns the map unchanged.
- `"rejects a module that is not a listed flow or job"`: `assert_raise ArgumentError, ~r/worker_options.*not listed in :flows or :jobs/`.
- `"rejects invalid per-worker keys"`: `%{SimpleFlow => [timeout: 5]}` raises `ArgumentError`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/pgflow/worker_options_test.exs test/pgflow/config_test.exs`
Expected: FAIL (`PgFlow.WorkerOptions` undefined; unknown option `:worker_options`).

- [ ] **Step 3: Implement `PgFlow.WorkerOptions` and the config key**

Use a NimbleOptions schema for the two keys inside `WorkerOptions` and re-raise its errors as `ArgumentError`. In `PgFlow.Config`, add `worker_options: [type: {:map, :atom, :keyword_list}, default: %{}, doc: ...]`, and after NimbleOptions validation call a private `validate_worker_options!(config)` that checks every key is in `config[:flows] ++ config[:jobs]` and runs `WorkerOptions.validate!/1` on every value. Add the option to the moduledoc list.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/pgflow/worker_options_test.exs test/pgflow/config_test.exs`
Expected: PASS

- [ ] **Step 5: Leave changes uncommitted**

Suggested message: `feat: Add per-worker option validation and resolution`

---

### Task 2: Supervisor starts workers with resolved options and keeps them across restarts

**Files:**
- Modify: `lib/pgflow/worker_supervisor.ex` (`start_worker/2`, `do_start_worker/2`, `start_worker_process`, moduledoc)
- Modify: `lib/pgflow/worker/server.ex` (state fields `configured_max_concurrency`, `configured_batch_size`; `:check` call, needed by this task's tests)
- Test: `test/pgflow/worker/worker_options_supervision_test.exs`

**Interfaces:**
- Consumes: `PgFlow.WorkerOptions.split/1`, `validate!/1`, `resolve/3` (Task 1).
- Produces:
  - `PgFlow.WorkerSupervisor.start_worker(module(), keyword()) :: {:ok, pid()} | {:error, {:conflicting_worker_options, pid()}} | {:error, term()}`. Accepts `:repo` plus the allowlisted keys.
  - `PgFlow.WorkerSupervisor.start_worker_process(module(), repo :: module(), overrides :: keyword()) :: GenServer.on_start()`. The 2-arity form remains, delegating with `[]`, so existing callers and tests keep working.
  - `PgFlow.Worker.Server.check(pid()) :: map()` with exactly the keys in the spec's `check_worker/1` result except `:module` and `:pid`: `lifecycle`, `max_concurrency`, `batch_size`, `configured_max_concurrency`, `configured_batch_size`, `active`.
  - Server state keys `:configured_max_concurrency` and `:configured_batch_size`, set in `init/1` from the start config's `max_concurrency`/`batch_size`.

- [ ] **Step 1: Write the failing tests**

New integration file, `@moduletag :integration`, setup copied from `test/pgflow/flow_starter_test.exs` lines 55-92 (sandbox shared mode, `pgflow_tests.reset_db()`, `{Task.Supervisor, name: PgFlow.TaskSupervisor}`, `{WorkerSupervisor, worker_config}`) plus the persistent-term snapshot/restore from Global Constraints, with `worker_config` including `max_concurrency: 8, batch_size: 8` and `worker_options: %{PgFlow.TestFlows.SimpleFlow => [max_concurrency: 3]}`. Compile `SimpleFlow` the way `server_test.exs`'s `compile_flow/1` does. Tests:
- `"configured worker_options set the worker's limits"`: `{:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)`; `assert %{max_concurrency: 3, batch_size: 8, configured_max_concurrency: 3, configured_batch_size: 8} = Server.check(pid)`.
- `"start_worker opts override worker_options"`: `start_worker(SimpleFlow, max_concurrency: 2, batch_size: 1)` gives `%{max_concurrency: 2, batch_size: 1}`.
- `"a crashed worker restarts with its resolved options"`: start with `max_concurrency: 2`; `Process.exit(pid, :kill)`; wait (use `PgFlow.Test.WaitHelpers`) until `WorkerSupervisor.find_worker(SimpleFlow)` returns a new pid; `assert %{max_concurrency: 2} = Server.check(new_pid)`.
- `"restarting an existing worker with the same or no options returns it"`: after `start_worker(SimpleFlow, max_concurrency: 2)`, both `start_worker(SimpleFlow)` and `start_worker(SimpleFlow, max_concurrency: 2)` return `{:ok, same_pid}`.
- `"restarting an existing worker with different options is rejected"`: then `start_worker(SimpleFlow, max_concurrency: 5)` returns `{:error, {:conflicting_worker_options, same_pid}}`.
- `"only supplied keys are compared"`: after `start_worker(SimpleFlow, max_concurrency: 2, batch_size: 1)`, `start_worker(SimpleFlow, batch_size: 1)` returns `{:ok, same_pid}`; after `Server.scale(pid, max_concurrency: 6)` (Task 3 adds `scale/2`; until then, skip this assertion with `@tag :skip` and remove the tag in Task 3), `start_worker(SimpleFlow, max_concurrency: 2)` still returns `{:ok, same_pid}` because comparison uses `configured_*`, not runtime values.
- `"concurrent first starts create one worker"`: spawn 5 processes that each wait on a `:go` message (barrier), send `:go` to all, then each calls `WorkerSupervisor.start_worker(SimpleFlow)` and reports the result; all five are `{:ok, pid}` with one distinct pid, and `DynamicSupervisor.count_children(WorkerSupervisor).active == 1`.
- `"invalid start options raise"`: `assert_raise ArgumentError` for `max_concurrency: 0` and for the typo `max_concurency: 4`.
- `"repo: still works"`: `start_worker(SimpleFlow, repo: TestRepo)` returns `{:ok, pid}` (the `FlowStarter` call shape).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/pgflow/worker/worker_options_supervision_test.exs`
Expected: FAIL (`Server.check/1` undefined; options ignored).

- [ ] **Step 3: Implement**

- `start_worker/2`: `{worker_opts, rest} = WorkerOptions.split(opts)`; raise `ArgumentError` when `rest` has any key other than `:repo`; `WorkerOptions.validate!(worker_opts)`; then run the find-or-start under `:global.trans({{__MODULE__, flow_module}, self()}, fn -> ... end, [node()])` (the requester id must be `self()`, or same-flow callers share the lock). For an existing worker, compare only the keys present in `worker_opts` against `Server.check/1`'s `configured_max_concurrency`/`configured_batch_size`; any mismatch returns `{:error, {:conflicting_worker_options, pid}}`.
- `do_start_worker`: child spec start is `{__MODULE__, :start_worker_process, [flow_module, repo, worker_opts]}` so DynamicSupervisor restarts reuse the same overrides.
- `start_worker_process/3`: build `worker_config` as today, but take `max_concurrency`/`batch_size` from `WorkerOptions.resolve(config, flow_module, overrides)`.
- `Server`: set the two `configured_*` state fields in `init/1`; add `check/1` (`GenServer.call(pid, :check)`) and its `handle_call(:check, ...)` reply (`active: map_size(state.active_tasks)`, `lifecycle: Lifecycle.current(state.lifecycle)`).

- [ ] **Step 4: Run tests to verify they pass, plus the existing worker and starter suites**

Run: `mix test test/pgflow/worker/worker_options_supervision_test.exs test/pgflow/worker/server_test.exs test/pgflow/flow_starter_test.exs`
Expected: PASS

- [ ] **Step 5: Leave changes uncommitted**

Suggested message: `feat: Start workers with per-worker options`

---

### Task 3: Runtime scaling in the worker

**Files:**
- Modify: `lib/pgflow/worker/server.ex` (`scale/2` client function, `handle_call({:scale, opts}, ...)`)
- Test: `test/pgflow/worker/scaling_test.exs`

**Interfaces:**
- Consumes: `Server.check/1` and the `configured_*` state (Task 2).
- Produces: `PgFlow.Worker.Server.scale(pid(), keyword()) :: {:ok, map()} | {:error, :not_running}`. `opts` is already validated by the caller (Task 4). The success map is the same shape as `check/1`.

- [ ] **Step 1: Write the failing tests**

Integration file, `@moduletag :integration`, using `server_test.exs`'s setup and its `start_worker/3` helper pattern (copy the private helper; it builds the worker config map directly) plus a gated fixture flow `PgFlow.Test.GatedFlow` defined in the test module (one root step following the gated-handler pattern in Global Constraints) and compiled with the same `compile_flow/1` approach. In every test, start the flow runs **before** starting the worker so the worker's initial poll finds them. Tests:
- `"scaling up starts queued tasks immediately"` (run for `signal_strategy: :polling` with `min_poll_interval`/`max_poll_interval: 60_000`, and for `:notify` with `notify_fallback_interval: 600_000`, so neither a scheduled poll nor the fallback can explain the result): worker `max_concurrency: 1`, 3 runs; receive one `:task_started`; `{:ok, %{max_concurrency: 3}} = Server.scale(pid, max_concurrency: 3)`; `assert_receive {:task_started, _, _}` twice within 1_000ms; release all; all runs complete.
- `"scaling down lets active tasks finish and limits new claims"`: worker `max_concurrency: 3`, 5 runs; receive three `:task_started`; `Server.scale(pid, max_concurrency: 1)`; `refute_receive {:task_started, _, _}, 300`; release one original task and `refute_receive {:task_started, _, _}, 300` (2 still active, above the limit of 1); release the second and refute again (1 active, at the limit); release the third and `assert_receive` exactly one new `:task_started`, then `refute_receive` another for 300ms; release everything; all 5 runs complete.
- `"scale changes batch_size"`: `{:ok, %{batch_size: 2, configured_batch_size: 10}} = Server.scale(pid, batch_size: 2)`.
- `"a stopping worker refuses to scale"`: one run; receive `:task_started`; `spawn(fn -> Server.stop(pid) end)`; wait (WaitHelpers) until `Server.check(pid).lifecycle == :stopping`; `assert {:error, :not_running} = Server.scale(pid, max_concurrency: 5)`; `check(pid).max_concurrency` unchanged; release the task.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/pgflow/worker/scaling_test.exs`
Expected: FAIL (`Server.scale/2` undefined).

- [ ] **Step 3: Implement `Server.scale/2`**

`handle_call({:scale, opts}, ...)`: when `Lifecycle.can_accept_work?/1` is false reply `{:error, :not_running}`; otherwise merge `opts` into `max_concurrency`/`batch_size`, and when the new `max_concurrency` exceeds `map_size(state.active_tasks)` `send(self(), :poll_now)` (works for both strategies; `:poll_now` already cancels the pending poll timer). Reply `{:ok, check_map(state)}`; reuse the private builder behind `check/1`. `poll_and_dispatch/1` already bounds claims by `min(available_slots, batch_size)`; do not change it.

- [ ] **Step 4: Run tests to verify they pass**

Remove the `@tag :skip` on Task 2's `"only supplied keys are compared"` test, then run:

Run: `mix test test/pgflow/worker/scaling_test.exs test/pgflow/worker/server_test.exs test/pgflow/worker/worker_options_supervision_test.exs`
Expected: PASS

- [ ] **Step 5: Leave changes uncommitted**

Suggested message: `feat: Scale a running worker's concurrency`

---

### Task 4: Public `PgFlow.scale_worker/2` and `PgFlow.check_worker/1`

**Files:**
- Modify: `lib/pgflow.ex` (`start_worker/2` docs, new `scale_worker/2`, `check_worker/1`)
- Modify: `lib/pgflow/worker_supervisor.ex` (`scale_worker/2`, `check_worker/1` delegations through `find_worker/1`)
- Test: `test/pgflow/worker/public_scaling_test.exs`

**Interfaces:**
- Consumes: `WorkerOptions.validate_scale!/1` (Task 1), `Server.scale/2`, `Server.check/1` (Tasks 2-3), `WorkerSupervisor.find_worker/1`.
- Produces:
  - `PgFlow.scale_worker(module(), keyword()) :: {:ok, map()} | {:error, :not_found | :not_running}`; raises `ArgumentError` on invalid options before looking up the worker (Oban validates first). The `{:ok, map}` carries the same keys as `check_worker/1`, including `module:` and `pid:`.
  - `PgFlow.check_worker(module()) :: map() | nil`; the map adds `module:` and `pid:` to `Server.check/1`'s keys.

- [ ] **Step 1: Write the failing tests**

Integration file with the Task 2 setup (`WorkerSupervisor` started, `worker_options: %{SimpleFlow => [max_concurrency: 3]}`, persistent-term snapshot/restore). Tests, named after Oban's (`test/oban_test.exs`, `describe "scale_queue/2"` and `"check_queue/2"`):
- `"validating options"`: `assert_raise ArgumentError` for `scale_worker(SimpleFlow, [])`, `max_concurrency: -1`, `max_concurrency: 0`, `batch_size: nil`, `paused: true`.
- `"scaling workers up"`: `{:ok, pid} = WorkerSupervisor.start_worker(SimpleFlow)`; `assert {:ok, %{module: SimpleFlow, pid: ^pid, max_concurrency: 6}} = PgFlow.scale_worker(SimpleFlow, max_concurrency: 6)`; `assert %{module: SimpleFlow, max_concurrency: 6, configured_max_concurrency: 3} = PgFlow.check_worker(SimpleFlow)`.
- `"checking a worker that isn't running locally"`: `assert PgFlow.check_worker(SimpleFlow) == nil` before start.
- `"trying to scale workers that don't exist"`: `assert {:error, :not_found} = PgFlow.scale_worker(SimpleFlow, max_concurrency: 2)`.
- `"scaling resets to configuration after a restart"`: scale to 6, `Process.exit(pid, :kill)`, wait for the restarted pid, `assert %{max_concurrency: 3} = PgFlow.check_worker(SimpleFlow)`.
- `"worker options leave the persisted flow definition unchanged"`: read `pgflow.flows`/`pgflow.steps` rows (`opt_max_attempts`, `opt_base_delay`, `opt_timeout`, `opt_start_delay`) for the compiled flow before starting; start with `worker_options` and scale; read again and assert equal; the worker's lifecycle is `:running` (bootstrap verified the shape).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mix test test/pgflow/worker/public_scaling_test.exs`
Expected: FAIL (`PgFlow.scale_worker/2` undefined).

- [ ] **Step 3: Implement the public functions**

`PgFlow.scale_worker/2` calls `WorkerOptions.validate_scale!/1`, then `WorkerSupervisor.scale_worker/2`, which returns `{:error, :not_found}` when `find_worker/1` is nil, else `Server.scale/2` with its success map merged with `%{module: m, pid: pid}`. `check_worker/1` returns nil or `Map.merge(Server.check(pid), %{module: m, pid: pid})`. A worker that exits between lookup and call returns `{:error, :not_found}` / `nil`: catch only `:exit` reasons `:noproc`, `{:noproc, _}`, `:normal`, `{:normal, _}`, `{:shutdown, _}`; let `{:timeout, _}` and every other exit propagate. Docs follow `lib/pgflow.ex`'s existing style, with examples mirroring Oban's `scale_queue` docs: scale up, scale down while tasks finish, runtime-only reset.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mix test test/pgflow/worker/public_scaling_test.exs`
Expected: PASS

- [ ] **Step 5: Leave changes uncommitted**

Suggested message: `feat: Add PgFlow.scale_worker/2 and check_worker/1`

---

### Task 5: Docs, version 0.6.0 and full verification

**Files:**
- Modify: `README.md` (configure section: `worker_options` example; new "Scaling workers at runtime" section; dependency line `{:pgflow, "~> 0.6.0"}`)
- Modify: `docs/ARCHITECTURE.md` (Configuration block adds `worker_options`; one paragraph on runtime scaling being in-process only)
- Modify: `docs/ELIXIR_VS_SUPABASE.md` (table row: per-worker concurrency = TS `EdgeWorker.start(flow, { maxConcurrent })`)
- Modify: `mix.exs` (`@version "0.6.0"`)

- [ ] **Step 1: Update docs and version as listed**

The README scaling section states the four contract points: local only, runtime only (resets on restart), scale-down never interrupts active tasks, scale-up polls immediately.

- [ ] **Step 2: Run the full suite and quality checks**

Run: `docker compose -f test/support/db/compose.yaml up -d && PGFLOW_REQUIRE_DB=1 mix test && mix quality && mix doctor`
Expected: all tests pass; quality and doctor clean for the changed modules.

- [ ] **Step 3: Hand off**

Leave everything uncommitted. Report the suggested messages from Tasks 1-5 (this task: `docs: Document per-worker options and runtime scaling`), and ask Chase before any commit, tag, push or `mix hex.publish`.
