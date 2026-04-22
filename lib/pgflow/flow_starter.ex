defmodule PgFlow.FlowStarter do
  @moduledoc """
  Supervised GenServer that registers flow and job modules with exponential
  backoff and jitter.

  Replaces the earlier fire-and-forget `Task` approach whose
  `restart: :temporary` strategy turned any transient registration failure
  into permanent silent job non-execution.

  Each module goes through up to four phases:

    1. `:registry` — `PgFlow.FlowRegistry.register/1`
    2. `:worker` — `PgFlow.WorkerSupervisor.start_worker/2`
    3. `:notify_db` — `PgFlow.Queries.Pgmq.enable_notify_insert/3` (`:notify` mode only)
    4. `:notify_register` — `PgFlow.Signal.Notify.register_worker/2` (`:notify` mode only)

  All four operations are idempotent, so retries are safe.

  ## Retry model

    * Transient errors (DB connectivity, supervisor not yet running) retry
      indefinitely with exponential backoff + uniform jitter.
    * Permanent errors (invalid flow module, missing `__pgflow_definition__/0`,
      pgmq SQL missing) stop retries immediately.

  ## API

    * `status/0` — snapshot of all modules
    * `module_status/1` — single-module snapshot
    * `ready?/0` — every module has reached a terminal state (converged).
      True even if all modules are `:failed_permanent` — use `healthy?/0` to
      check whether any succeeded.
    * `healthy?/0` — at least one module succeeded (empty config counts as healthy).
    * `await_ready/1` — blocks until `ready?/0` is true or timeout. Accepts `:infinity`.
    * `retry_now/1` — force an immediate attempt (operational poke)

  ## Telemetry

    * `[:pgflow, :starter, :module, :attempt]`
    * `[:pgflow, :starter, :module, :success]`
    * `[:pgflow, :starter, :module, :retry_scheduled]`
    * `[:pgflow, :starter, :module, :failed_permanent]`
    * `[:pgflow, :starter, :ready]`
  """

  use GenServer
  require Logger

  alias PgFlow.{FlowRegistry, WorkerSupervisor}
  alias PgFlow.Queries.Pgmq, as: PgmqQueries
  alias PgFlow.Signal

  @default_backoff_base_ms 1_000
  @default_backoff_max_ms 30_000
  @default_backoff_jitter_ratio 0.20

  defmodule ModuleState do
    @moduledoc false

    @type status :: :pending | :retrying | :succeeded | :failed_permanent
    @type phase :: :registry | :worker | :notify_db | :notify_register
    @type error_class :: :transient | :permanent

    @type t :: %__MODULE__{
            module: module(),
            type: String.t(),
            status: status(),
            attempts: non_neg_integer(),
            last_error:
              nil
              | %{class: error_class(), phase: phase(), reason: term(), at: DateTime.t()},
            updated_at: DateTime.t()
          }

    defstruct [
      :module,
      :type,
      :status,
      :attempts,
      :last_error,
      :updated_at
    ]
  end

  defmodule State do
    @moduledoc false

    @type backoff_opts :: %{
            base_ms: non_neg_integer(),
            max_ms: non_neg_integer(),
            jitter_ratio: float()
          }

    @type t :: %__MODULE__{
            repo: module(),
            signal_strategy: :polling | :notify,
            notify_throttle_ms: non_neg_integer(),
            backoff: backoff_opts(),
            modules: %{optional(module()) => PgFlow.FlowStarter.ModuleState.t()},
            timers: %{optional(module()) => {reference(), non_neg_integer()}},
            next_gen: non_neg_integer(),
            waiters: [{GenServer.from(), reference()}],
            started_at: DateTime.t(),
            ready_at: DateTime.t() | nil
          }

    defstruct [
      :repo,
      :signal_strategy,
      :notify_throttle_ms,
      :backoff,
      :modules,
      # timers: %{module => {timer_ref, generation}} — generation guards against
      # stale {:attempt, module, gen} messages that were already in the mailbox
      # when a Process.cancel_timer/1 call returned false.
      :timers,
      # Monotonically increasing generation counter. Every schedule_attempt/3
      # call consumes the next value.
      :next_gen,
      # waiters: [{GenServer.from(), reference()}] — monitor ref per waiter so
      # we can prune on :DOWN.
      :waiters,
      :started_at,
      :ready_at
    ]
  end

  # ── Child spec ─────────────────────────────────────────────────────

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  # ── Client API ─────────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Snapshot of the starter state."
  @spec status() :: map()
  def status, do: GenServer.call(__MODULE__, :status)

  @doc "Snapshot of a single module's state, or nil if not registered."
  @spec module_status(module()) :: ModuleState.t() | nil
  def module_status(module) do
    GenServer.call(__MODULE__, {:module_status, module})
  end

  @doc """
  Returns true when every module has reached a terminal state
  (`:succeeded` or `:failed_permanent`). Empty config is trivially ready.

  This is the "starter has converged" signal, suitable for K8s readiness
  probes. For "pgflow is actually functional" checks, use `healthy?/0`.
  """
  @spec ready?() :: boolean()
  def ready?, do: GenServer.call(__MODULE__, :ready?)

  @doc """
  Returns true when at least one module succeeded (or the config is empty).
  """
  @spec healthy?() :: boolean()
  def healthy?, do: GenServer.call(__MODULE__, :healthy?)

  @doc """
  Blocks until `ready?/0` is true or `timeout` ms elapse.

  Accepts `:infinity` to block indefinitely.

  Returns `:ok` when ready, `{:error, :timeout}` on timeout.
  """
  @spec await_ready(timeout()) :: :ok | {:error, :timeout}
  def await_ready(timeout \\ 5_000) do
    call_timeout = if timeout == :infinity, do: :infinity, else: timeout + 100
    GenServer.call(__MODULE__, :await_ready, call_timeout)
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
  end

  @doc "Force an immediate retry for a module (bypasses current backoff timer)."
  @spec retry_now(module()) :: :ok
  def retry_now(module) do
    GenServer.cast(__MODULE__, {:retry_now, module})
  end

  # ── GenServer callbacks ────────────────────────────────────────────

  @impl GenServer
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    flows = Keyword.get(opts, :flows, [])
    jobs = Keyword.get(opts, :jobs, [])

    backoff = %{
      base_ms: Keyword.get(opts, :backoff_base_ms, @default_backoff_base_ms),
      max_ms: Keyword.get(opts, :backoff_max_ms, @default_backoff_max_ms),
      jitter_ratio: Keyword.get(opts, :backoff_jitter_ratio, @default_backoff_jitter_ratio)
    }

    modules =
      (Enum.map(flows, &initial_module_state(&1, "flow")) ++
         Enum.map(jobs, &initial_module_state(&1, "job")))
      |> Enum.reduce(%{}, fn ms, acc -> Map.put(acc, ms.module, ms) end)

    state = %State{
      repo: repo,
      signal_strategy: Keyword.get(opts, :signal_strategy, :polling),
      notify_throttle_ms: Keyword.get(opts, :notify_throttle_ms, 250),
      backoff: backoff,
      modules: modules,
      timers: %{},
      next_gen: 1,
      waiters: [],
      started_at: DateTime.utc_now()
    }

    {:ok, state, {:continue, :bootstrap}}
  end

  @impl GenServer
  def handle_continue(:bootstrap, state) do
    state =
      Enum.reduce(Map.keys(state.modules), state, fn module, acc ->
        schedule_attempt(acc, module, 0)
      end)

    {:noreply, state}
  end

  @impl GenServer
  # Generation guard: drop stale messages whose timer was superseded by a
  # subsequent schedule_attempt/3 (e.g. retry_now/1 racing with an already-
  # delivered timer message).
  def handle_info({:attempt, module, gen}, state) do
    case Map.get(state.timers, module) do
      {_ref, ^gen} ->
        state = %{state | timers: Map.delete(state.timers, module)}
        run_module_attempt(module, state)

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    waiters = Enum.reject(state.waiters, fn {_from, r} -> r == ref end)
    {:noreply, %{state | waiters: waiters}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp run_module_attempt(module, state) do
    ms = Map.fetch!(state.modules, module)

    start_time = System.monotonic_time()
    emit(:attempt, module, ms, %{attempt: ms.attempts + 1})

    case run_phases(ms, state) do
      {:ok, updated_ms} ->
        duration_ms =
          System.convert_time_unit(
            System.monotonic_time() - start_time,
            :native,
            :millisecond
          )

        emit(:success, module, updated_ms, %{duration_ms: duration_ms})
        Logger.info("FlowStarter: registered #{updated_ms.type} #{inspect(module)}")

        state = put_module(state, updated_ms) |> maybe_mark_ready()
        {:noreply, state}

      {:transient, reason, phase, updated_ms} ->
        attempts = updated_ms.attempts + 1

        updated_ms = %{
          updated_ms
          | attempts: attempts,
            status: :retrying,
            last_error: %{
              class: :transient,
              phase: phase,
              reason: reason,
              at: DateTime.utc_now()
            },
            updated_at: DateTime.utc_now()
        }

        delay = backoff_ms(attempts, state.backoff)
        state = put_module(state, updated_ms) |> schedule_attempt(module, delay)

        emit(:retry_scheduled, module, updated_ms, %{delay_ms: delay, attempt: attempts})

        Logger.warning(
          "FlowStarter: transient failure for #{inspect(module)} at #{phase} (attempt #{attempts}), retrying in #{delay}ms: #{inspect(reason)}"
        )

        {:noreply, state}

      {:permanent, reason, phase, updated_ms} ->
        updated_ms = %{
          updated_ms
          | status: :failed_permanent,
            last_error: %{
              class: :permanent,
              phase: phase,
              reason: reason,
              at: DateTime.utc_now()
            },
            updated_at: DateTime.utc_now()
        }

        emit(:failed_permanent, module, updated_ms)

        Logger.error(
          "FlowStarter: permanent failure for #{inspect(module)} at #{phase}: #{inspect(reason)}"
        )

        state = put_module(state, updated_ms) |> maybe_mark_ready()
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply, snapshot(state), state}
  end

  def handle_call({:module_status, module}, _from, state) do
    {:reply, Map.get(state.modules, module), state}
  end

  def handle_call(:ready?, _from, state) do
    {:reply, ready_now?(state), state}
  end

  def handle_call(:healthy?, _from, state) do
    {:reply, healthy_now?(state), state}
  end

  def handle_call(:await_ready, {pid, _tag} = from, state) do
    if ready_now?(state) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)
      {:noreply, %{state | waiters: [{from, ref} | state.waiters]}}
    end
  end

  @impl GenServer
  def handle_cast({:retry_now, module}, state) do
    case Map.get(state.modules, module) do
      nil -> {:noreply, state}
      _ms -> {:noreply, schedule_attempt(state, module, 0)}
    end
  end

  # ── Phase execution ────────────────────────────────────────────────

  defp run_phases(%ModuleState{} = ms, state) do
    with {:ok, ms} <- run_phase(:registry, ms, state),
         {:ok, ms, worker_pid} <- run_worker_phase(ms, state),
         {:ok, ms} <- maybe_run_notify_phases(ms, state, worker_pid) do
      {:ok, %{ms | status: :succeeded, last_error: nil, updated_at: DateTime.utc_now()}}
    else
      {:error, class, reason, phase} -> {class, reason, phase, ms}
    end
  end

  # Phase 1 — registry. FlowRegistry.register/1 returns tuples; permanent
  # errors are invalid or unloaded modules.
  defp run_phase(:registry, ms, _state) do
    case FlowRegistry.register(ms.module) do
      :ok -> {:ok, ms}
      {:error, {:not_loaded, _}} = err -> {:error, :permanent, err, :registry}
      {:error, {:invalid_flow_module, _}} = err -> {:error, :permanent, err, :registry}
    end
  end

  # Phase 2 — worker. WorkerSupervisor returns tuples; anything else is a
  # real bug and should crash the starter (supervisor restart re-tries
  # registration, which is idempotent).
  defp run_worker_phase(ms, state) do
    case WorkerSupervisor.start_worker(ms.module, repo: state.repo) do
      {:ok, pid} -> {:ok, ms, pid}
      {:error, reason} -> {:error, :transient, reason, :worker}
    end
  end

  defp maybe_run_notify_phases(ms, %State{signal_strategy: :notify} = state, worker_pid) do
    slug = flow_slug(ms.module)

    with {:ok, ms} <- run_notify_db(ms, state, slug) do
      run_notify_register(ms, slug, worker_pid)
    end
  end

  defp maybe_run_notify_phases(ms, _state, _worker_pid), do: {:ok, ms}

  # Phase 3 — pgmq enable_notify_insert. Returns tuples; deterministic SQL
  # faults are permanent, connection-layer hiccups are transient.
  defp run_notify_db(ms, state, slug) do
    case PgmqQueries.enable_notify_insert(state.repo, slug, state.notify_throttle_ms) do
      :ok -> {:ok, ms}
      {:error, reason} -> {:error, classify_db_error(reason), reason, :notify_db}
    end
  end

  # Phase 4 — Signal.Notify register. Uses GenServer.call which can `:exit`
  # with `:noproc` during a startup race before Signal.Notify is running.
  # That exit is a legitimate transient signal; catch it here.
  defp run_notify_register(ms, slug, worker_pid) do
    case Signal.Notify.register_worker(slug, worker_pid) do
      :ok -> {:ok, ms}
      {:error, reason} -> {:error, :transient, reason, :notify_register}
    end
  catch
    :exit, {:noproc, _} -> {:error, :transient, :noproc, :notify_register}
    :exit, :noproc -> {:error, :transient, :noproc, :notify_register}
    :exit, {:timeout, _} -> {:error, :transient, :timeout, :notify_register}
  end

  # ── Error classification ───────────────────────────────────────────

  # Postgrex errors: deterministic SQL/config faults are permanent; most
  # connection-layer errors are transient.
  @permanent_pg_codes [
    :undefined_function,
    :undefined_table,
    :undefined_object,
    :syntax_error,
    :insufficient_privilege,
    :invalid_parameter_value,
    :feature_not_supported
  ]

  defp classify_db_error(%Postgrex.Error{postgres: %{code: code}})
       when code in @permanent_pg_codes,
       do: :permanent

  defp classify_db_error(_reason), do: :transient

  # ── State helpers ──────────────────────────────────────────────────

  defp initial_module_state(module, type) when is_atom(module) do
    %ModuleState{
      module: module,
      type: type,
      status: :pending,
      attempts: 0,
      last_error: nil,
      updated_at: DateTime.utc_now()
    }
  end

  defp put_module(state, %ModuleState{module: module} = ms) do
    %{state | modules: Map.put(state.modules, module, ms)}
  end

  defp schedule_attempt(state, module, delay_ms) do
    state = cancel_timer(state, module)
    gen = state.next_gen
    ref = Process.send_after(self(), {:attempt, module, gen}, delay_ms)

    %{
      state
      | timers: Map.put(state.timers, module, {ref, gen}),
        next_gen: gen + 1
    }
  end

  defp cancel_timer(state, module) do
    case Map.pop(state.timers, module) do
      {nil, _} ->
        state

      {{ref, _gen}, timers} ->
        # cancel_timer/1 may return false if the message is already in our
        # mailbox; the generation guard in handle_info/2 drops those.
        _ = Process.cancel_timer(ref)
        %{state | timers: timers}
    end
  end

  defp flow_slug(module) do
    module.__pgflow_definition__().slug |> Atom.to_string()
  end

  # ── Readiness ──────────────────────────────────────────────────────

  # ready? = every module has reached a terminal state (converged).
  # Empty config is trivially ready.
  defp ready_now?(%State{modules: modules}) do
    map_size(modules) == 0 or
      Enum.all?(modules, fn {_m, ms} -> ms.status in [:succeeded, :failed_permanent] end)
  end

  # healthy? = at least one module succeeded (or config is empty).
  defp healthy_now?(%State{modules: modules}) do
    map_size(modules) == 0 or
      Enum.any?(modules, fn {_m, ms} -> ms.status == :succeeded end)
  end

  defp maybe_mark_ready(state) do
    if ready_now?(state) do
      state = flush_waiters(state)

      if state.ready_at do
        state
      else
        ready_at = DateTime.utc_now()
        # Set ready_at before emitting so any telemetry handler that calls
        # back into status/0 sees a consistent snapshot.
        state = %{state | ready_at: ready_at}
        emit_ready(state, ready_at)
        state
      end
    else
      state
    end
  end

  defp flush_waiters(%State{waiters: []} = state), do: state

  defp flush_waiters(%State{waiters: waiters} = state) do
    Enum.each(waiters, fn {from, ref} ->
      Process.demonitor(ref, [:flush])
      GenServer.reply(from, :ok)
    end)

    %{state | waiters: []}
  end

  # ── Backoff ────────────────────────────────────────────────────────

  # Exponential backoff with uniform ±jitter_ratio jitter. Mirrors the
  # adaptive polling backoff style in PgFlow.Worker.Server but is not
  # "decorrelated jitter" in the strict AWS sense.
  defp backoff_ms(attempt, %{base_ms: base, max_ms: max, jitter_ratio: j}) do
    raw = min(max, base * Integer.pow(2, attempt - 1))
    jitter = trunc(raw * j)
    offset = if jitter > 0, do: :rand.uniform(2 * jitter + 1) - jitter - 1, else: 0
    max(0, raw + offset)
  end

  # ── Observability ──────────────────────────────────────────────────

  defp emit(event, module, ms, measurements \\ %{}) do
    :telemetry.execute(
      [:pgflow, :starter, :module, event],
      Map.put(measurements, :system_time, System.system_time()),
      %{
        module: module,
        module_type: ms.type,
        status: ms.status,
        attempts: ms.attempts,
        error_class: get_in(ms.last_error || %{}, [:class]),
        phase: get_in(ms.last_error || %{}, [:phase]),
        reason: get_in(ms.last_error || %{}, [:reason])
      }
    )
  end

  defp emit_ready(state, ready_at) do
    :telemetry.execute(
      [:pgflow, :starter, :ready],
      %{
        system_time: System.system_time(),
        duration_ms: DateTime.diff(ready_at, state.started_at, :millisecond)
      },
      %{module_count: map_size(state.modules)}
    )

    Logger.info("FlowStarter: all modules reached terminal state")
  end

  defp snapshot(state) do
    %{
      ready?: ready_now?(state),
      healthy?: healthy_now?(state),
      started_at: state.started_at,
      ready_at: state.ready_at,
      modules:
        state.modules
        |> Map.values()
        |> Enum.map(&Map.take(&1, [:module, :type, :status, :attempts, :last_error, :updated_at]))
    }
  end
end
