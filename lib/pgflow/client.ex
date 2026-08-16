defmodule PgFlow.Client do
  @moduledoc """
  Client API for interacting with PgFlow runs.

  Provides functions to start flows, query run status, and wait for completion.

  ## Usage

      # Start a flow asynchronously
      {:ok, run_id} = PgFlow.Client.start_flow(:my_flow, %{"order_id" => 123})

      # Start a flow and wait for completion
      {:ok, run} = PgFlow.Client.start_flow_sync(:my_flow, %{"order_id" => 123}, timeout: 30_000)

      # Get run details
      {:ok, run} = PgFlow.Client.get_run(run_id)

      # Get run with step states preloaded
      {:ok, run} = PgFlow.Client.get_run_with_states(run_id)

  """

  import Ecto.Query

  alias PgFlow.Queries.Flows
  alias PgFlow.Schema.Run
  alias PgFlow.Telemetry

  @allowed_step_types ["single", "map"]
  @skip_modes [:fail, :skip, :skip_cascade]
  @skip_mode_strings ~w(fail skip skip_cascade skip-cascade)

  @doc """
  Starts a flow run with the given input.

  Calls the `pgflow.start_flow` SQL function which handles all initialization:
  - Creates run and step_states records
  - Handles map step initial_tasks
  - Broadcasts run:started event
  - Enqueues ready steps to pgmq
  - Handles empty cascades

  The flow can be specified by module name or slug atom/string.
  Returns `{:ok, run_id}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, run_id} = PgFlow.Client.start_flow(:process_order, %{"order_id" => 123})
      {:ok, run_id} = PgFlow.Client.start_flow("process_order", %{"order_id" => 123})
      {:ok, run_id} = PgFlow.Client.start_flow(MyApp.Flows.ProcessOrder, %{"order_id" => 123})

  """
  @spec start_flow(module() | atom() | String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def start_flow(flow_module_or_slug, input) when is_map(input) do
    with {:ok, repo} <- get_repo(),
         {:ok, flow_slug} <- resolve_slug(flow_module_or_slug),
         {:ok, run_id, run_snapshot} <- Flows.start_flow_with_run(repo, flow_slug, input) do
      :telemetry.execute(
        [:pgflow, :run, :started],
        %{system_time: System.system_time()},
        %{flow_slug: flow_slug, run_id: run_id}
      )

      emit_post_start(repo, flow_slug, run_id, run_snapshot)

      {:ok, run_id}
    end
  end

  @doc """
  Enqueues a background job immediately.

  Jobs are single-step flows, so this delegates to `start_flow/2`.
  """
  @spec enqueue(module(), map()) :: {:ok, String.t()} | {:error, term()}
  def enqueue(job_module, input) when is_atom(job_module) and is_map(input) do
    start_flow(job_module, input)
  end

  @doc """
  Enqueues a background job with options.

  Supported options:

    * `:delay_seconds` - non-negative integer seconds before the job is visible
    * `:scheduled_at` - `DateTime` when the job should become visible

  """
  @spec enqueue(module(), map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def enqueue(job_module, input, opts)
      when is_atom(job_module) and is_map(input) and is_list(opts) do
    cond do
      scheduled_at = Keyword.get(opts, :scheduled_at) ->
        enqueue_at(job_module, input, scheduled_at)

      delay_seconds = Keyword.get(opts, :delay_seconds) ->
        enqueue_in(job_module, input, delay_seconds)

      true ->
        enqueue(job_module, input)
    end
  end

  @doc """
  Enqueues a background job that becomes visible after `delay_seconds`.
  """
  @spec enqueue_in(module(), map(), non_neg_integer()) :: {:ok, String.t()} | {:error, term()}
  def enqueue_in(job_module, input, delay_seconds)
      when is_atom(job_module) and is_map(input) and is_integer(delay_seconds) and
             delay_seconds >= 0 do
    with {:ok, repo} <- get_repo(),
         {:ok, flow_slug} <- resolve_slug(job_module) do
      repo.transaction(fn -> start_delayed_run(repo, flow_slug, input, delay_seconds) end)
    end
  end

  def enqueue_in(_job_module, _input, _delay_seconds), do: {:error, :invalid_delay_seconds}

  @doc """
  Enqueues a background job that becomes visible at `scheduled_at`.

  Timestamps in the past are enqueued for immediate execution.
  """
  @spec enqueue_at(module(), map(), DateTime.t()) :: {:ok, String.t()} | {:error, term()}
  def enqueue_at(job_module, input, %DateTime{} = scheduled_at)
      when is_atom(job_module) and is_map(input) do
    enqueue_in(job_module, input, seconds_until(scheduled_at))
  end

  def enqueue_at(_job_module, _input, _scheduled_at), do: {:error, :invalid_scheduled_at}

  defp seconds_until(%DateTime{} = scheduled_at) do
    scheduled_at
    |> DateTime.diff(DateTime.utc_now(), :second)
    |> max(0)
  end

  defp start_delayed_run(repo, flow_slug, input, delay_seconds) do
    case start_and_delay_run(repo, flow_slug, input, delay_seconds) do
      {:ok, run_id} ->
        emit_run_started(flow_slug, run_id)
        run_id

      {:error, reason} ->
        repo.rollback(reason)
    end
  end

  defp start_and_delay_run(repo, flow_slug, input, delay_seconds) do
    with {:ok, run_id} <- Flows.start_flow(repo, flow_slug, input),
         :ok <- Flows.delay_run(repo, flow_slug, run_id, delay_seconds) do
      {:ok, run_id}
    end
  end

  defp emit_run_started(flow_slug, run_id) do
    :telemetry.execute(
      [:pgflow, :run, :started],
      %{system_time: System.system_time()},
      %{flow_slug: flow_slug, run_id: run_id}
    )
  end

  @doc """
  Starts a flow and waits for completion.

  Blocks until the flow completes or the timeout is reached. Returns the
  completed run on success or error.

  ## Options

    * `:timeout` - Maximum time to wait in milliseconds (default: 60_000)
    * `:poll_interval` - How often to check status in milliseconds (default: 500)

  ## Examples

      {:ok, run} = PgFlow.Client.start_flow_sync(:my_flow, %{"order_id" => 123})
      {:error, run} = PgFlow.Client.start_flow_sync(:failing_flow, %{})
      {:error, :timeout} = PgFlow.Client.start_flow_sync(:slow_flow, %{}, timeout: 1000)

  """
  @spec start_flow_sync(module() | atom() | String.t(), map(), keyword()) ::
          {:ok, Run.t()} | {:error, Run.t()} | {:error, :timeout} | {:error, term()}
  def start_flow_sync(flow_module_or_slug, input, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    poll_interval = Keyword.get(opts, :poll_interval, 500)

    with {:ok, run_id} <- start_flow(flow_module_or_slug, input),
         {:ok, repo} <- get_repo() do
      wait_for_completion(repo, run_id, timeout, poll_interval)
    end
  end

  @doc """
  Gets a run by ID.

  Returns `{:ok, run}` if found, or `{:error, :not_found}` if the run does not exist.

  ## Examples

      {:ok, run} = PgFlow.Client.get_run(run_id)
      {:error, :not_found} = PgFlow.Client.get_run("00000000-0000-0000-0000-000000000000")

  """
  @spec get_run(String.t()) :: {:ok, Run.t()} | {:error, :not_found} | {:error, term()}
  def get_run(run_id) do
    with {:ok, repo} <- get_repo() do
      case repo.get(Run, run_id) do
        nil -> {:error, :not_found}
        run -> {:ok, run}
      end
    end
  end

  @doc """
  Gets a run with all step states preloaded.

  Returns `{:ok, run}` with step_states association loaded, or `{:error, :not_found}`
  if the run does not exist.

  ## Examples

      {:ok, run} = PgFlow.Client.get_run_with_states(run_id)
      Enum.each(run.step_states, fn state ->
        IO.puts("\#{state.step_slug}: \#{state.status}")
      end)

  """
  @spec get_run_with_states(String.t()) ::
          {:ok, Run.t()} | {:error, :not_found} | {:error, term()}
  def get_run_with_states(run_id) do
    with {:ok, repo} <- get_repo() do
      query =
        from(r in Run,
          where: r.run_id == ^run_id,
          preload: [:step_states]
        )

      case repo.one(query) do
        nil -> {:error, :not_found}
        run -> {:ok, run}
      end
    end
  end

  @doc """
  Recompiles a flow definition at runtime.

  This is the primary API for runtime flow management. Unlike the compile-time
  DSL (`use PgFlow.Flow`), this function creates flow definitions from plain
  data - ideal for per-tenant automations where flows are defined dynamically.

  If the flow already exists, this operation is destructive: the existing
  definition and historical run/task data for the slug are deleted before
  recompiling.

  ## Options

    * `:max_attempts` - Maximum retry attempts (default: 3)
    * `:base_delay` - Base delay between retries in seconds (default: 1)
    * `:timeout` - Step timeout in seconds (default: 60)
    * `:steps` - **Required.** List of step definition maps, each with:
      * `:slug` - Step identifier (required)
      * `:deps` - List of dependency step slugs (default: [])
      * `:step_type` - Step type, e.g. `"single"` (default: `"single"`)
      * `:max_attempts` - Step-level retry override (optional)
      * `:base_delay` - Step-level delay override (optional)
      * `:timeout` - Step-level timeout override (optional)
      * `:start_delay` - Delay before step starts in seconds (optional)
      * `:if` - Map (JSON-encodable) the run's input must match for the step
        to execute. When unmet, the step is skipped (or fails/cascades,
        depending on `:when_unmet`) instead of running (optional)
      * `:if_not` - Map the run's input must **not** match for the step to
        execute; mutually exclusive intent with `:if` but not mutually
        validated (optional)
      * `:when_unmet` - What happens when `:if`/`:if_not` is not satisfied:
        `:fail`, `:skip`, or `:skip_cascade` (atoms or their string
        equivalents, including `"skip-cascade"`). Requires `:if` or
        `:if_not` to be set. Defaults to `:skip` at the database layer when
        omitted (optional)
      * `:when_exhausted` - What happens when the step exhausts its retries:
        `:fail`, `:skip`, or `:skip_cascade` (same accepted forms as
        `:when_unmet`). Defaults to `:fail` at the database layer when
        omitted (optional)

  ## Examples

      PgFlow.Client.upsert_flow("acct_123_hubspot_sync_v1",
        max_attempts: 3,
        steps: [
          %{slug: "reshape", deps: []},
          %{slug: "create_contact", deps: ["reshape"]}
        ]
      )
      # => {:ok, %{"status" => "compiled", "differences" => []}}

      PgFlow.Client.upsert_flow("acct_123_hubspot_sync_v1",
        steps: [
          %{
            slug: "premium_only",
            if: %{"plan" => "premium"},
            when_unmet: :skip_cascade,
            when_exhausted: :skip
          }
        ]
      )
      # => {:ok, %{"status" => "compiled", "differences" => []}}

  """
  @spec upsert_flow(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def upsert_flow(slug, opts) when is_binary(slug) and is_list(opts) do
    with {:ok, repo} <- get_repo(),
         :ok <- validate_runtime_slug(repo, slug),
         {:ok, {flow_opts, steps}} <- build_shape(repo, opts) do
      result = Flows.upsert_flow(repo, slug, flow_opts, steps)

      case result do
        {:ok, status_map} ->
          :telemetry.execute(
            [:pgflow, :flow, :ensured],
            %{system_time: System.system_time()},
            %{flow_slug: slug, status: status_map["status"]}
          )

          {:ok, status_map}

        error ->
          error
      end
    end
  end

  @doc """
  Deletes a flow and all associated data (runs, tasks, queue).

  This permanently removes the flow definition and all historical run data.
  Intended for cleaning up retired flow versions.

  ## Examples

      PgFlow.Client.delete_flow("acct_123_hubspot_sync_v1")
      # => :ok

  """
  @spec delete_flow(String.t()) :: :ok | {:error, term()}
  def delete_flow(slug) when is_binary(slug) do
    with {:ok, repo} <- get_repo(),
         :ok <- validate_runtime_slug(repo, slug) do
      result = Flows.delete_flow(repo, slug)

      case result do
        :ok ->
          :telemetry.execute(
            [:pgflow, :flow, :deleted],
            %{system_time: System.system_time()},
            %{flow_slug: slug}
          )

          :ok

        error ->
          error
      end
    end
  end

  @doc """
  Checks if a flow exists in the database.

  ## Examples

      PgFlow.Client.flow_exists?("acct_123_hubspot_sync_v1")
      # => {:ok, true}

  """
  @spec flow_exists?(String.t()) :: {:ok, boolean()} | {:error, term()}
  def flow_exists?(slug) when is_binary(slug) do
    with {:ok, repo} <- get_repo(),
         :ok <- validate_runtime_slug(repo, slug) do
      Flows.flow_exists?(repo, slug)
    end
  end

  # Private Functions

  # Decides post-start emissions from the run row `start_flow` itself
  # returned, never from a fresh read. That row is read back inside the same
  # implicit transaction that created the run, evaluated conditions
  # (`cascade_resolve_conditions`), completed taskless steps, and enqueued
  # the initial tasks — so it is exactly what `pgflow.start_flow` decided,
  # nothing a worker did afterwards. Re-querying after the statement commits
  # would race a fast worker: it could observe a run the worker had already
  # failed for a genuine handler error and misreport it as "condition
  # unmet". Because task execution requires a task row to be visible to a
  # worker, which can't happen until this statement's transaction commits,
  # a `"failed"` status on this snapshot can only be the synchronous
  # `when_unmet: :fail` path inside `cascade_resolve_conditions` — no task
  # has had a chance to run, let alone fail, yet.
  defp emit_post_start(repo, flow_slug, run_id, run_snapshot) do
    Telemetry.emit_skipped_steps(repo, flow_slug, run_id)

    case run_snapshot do
      %{status: "completed", output: output} ->
        :telemetry.execute(
          [:pgflow, :run, :completed],
          %{system_time: System.system_time()},
          %{flow_slug: flow_slug, run_id: run_id, output: output}
        )

      %{status: "failed"} ->
        :telemetry.execute(
          [:pgflow, :run, :failed],
          %{system_time: System.system_time()},
          %{flow_slug: flow_slug, run_id: run_id, error: "condition unmet"}
        )

      _ ->
        :ok
    end
  end

  defp build_shape(repo, opts) do
    case Keyword.fetch(opts, :steps) do
      :error ->
        {:error, :steps_required}

      {:ok, steps} when is_list(steps) ->
        flow_opts = %{
          "max_attempts" => Keyword.get(opts, :max_attempts, 3),
          "base_delay" => Keyword.get(opts, :base_delay, 1),
          "timeout" => Keyword.get(opts, :timeout, 60)
        }

        with {:ok, step_maps} <- normalize_steps(repo, steps),
             :ok <- validate_step_dependencies(step_maps) do
          {:ok, {flow_opts, step_maps}}
        end

      {:ok, _invalid} ->
        {:error, :steps_must_be_list}
    end
  end

  defp get_repo do
    # Try persistent_term first (set by supervisor), then application env
    case :persistent_term.get({PgFlow, :repo}, nil) do
      nil ->
        case Application.get_env(:pgflow, :repo) do
          nil -> {:error, "Repo not configured"}
          repo -> {:ok, repo}
        end

      repo ->
        {:ok, repo}
    end
  end

  defp resolve_slug(module) when is_atom(module) do
    # `function_exported?/3` does not trigger code loading, so an unloaded
    # flow module would silently fall through to `Atom.to_string(module)` and
    # produce an invalid slug like "Elixir.MyApp.Flows.Foo", violating the
    # pgflow.flows FK at runtime. Force-load first.
    loaded? = Code.ensure_loaded?(module)

    cond do
      loaded? and function_exported?(module, :__pgflow_slug__, 0) ->
        {:ok, Atom.to_string(module.__pgflow_slug__())}

      loaded? ->
        # Loaded module without __pgflow_slug__/0 — preserve legacy behaviour
        # that stringifies the module name as the slug.
        {:ok, Atom.to_string(module)}

      elixir_module_alias?(module) ->
        # Unloaded Elixir module alias (typo, uncompiled, missing from code
        # path). Refuse to fall through to module-name stringification.
        {:error, {:unknown_flow, module}}

      true ->
        # Bare atom slug like :my_flow, not a module alias.
        {:ok, Atom.to_string(module)}
    end
  end

  defp resolve_slug(slug) when is_binary(slug) do
    {:ok, slug}
  end

  defp elixir_module_alias?(atom) do
    atom
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end

  defp validate_runtime_slug(repo, slug) do
    case Flows.valid_slug?(repo, slug) do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, {:invalid_slug, slug}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_steps(repo, steps) do
    Enum.reduce_while(steps, {:ok, []}, fn step, {:ok, acc} ->
      case normalize_step(repo, step) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized_steps} -> {:ok, Enum.reverse(normalized_steps)}
      error -> error
    end
  end

  defp normalize_step(repo, step) when is_map(step) do
    with {:ok, slug} <- fetch_required_step_slug(step),
         :ok <- validate_runtime_slug(repo, slug),
         {:ok, deps} <- normalize_step_deps(step),
         {:ok, step_type} <- normalize_step_type(step),
         :ok <- validate_optional_deps(repo, deps),
         {:ok, condition_opts} <- normalize_step_conditions(step, slug) do
      {:ok,
       Map.merge(
         %{
           "slug" => slug,
           "deps" => deps,
           "step_type" => step_type,
           "max_attempts" => get_step_value(step, :max_attempts),
           "base_delay" => get_step_value(step, :base_delay),
           "timeout" => get_step_value(step, :timeout),
           "start_delay" => get_step_value(step, :start_delay)
         },
         condition_opts
       )}
    end
  end

  defp normalize_step(_repo, _invalid), do: {:error, :invalid_step}

  defp fetch_required_step_slug(step) do
    case get_step_value(step, :slug) do
      nil -> {:error, :step_slug_required}
      slug when is_atom(slug) -> {:ok, Atom.to_string(slug)}
      slug when is_binary(slug) -> {:ok, slug}
      slug -> {:error, {:invalid_step_slug, slug}}
    end
  end

  defp normalize_step_deps(step) do
    deps = get_step_value(step, :deps, [])

    if is_list(deps) do
      normalized_deps =
        Enum.map(deps, fn
          dep when is_atom(dep) -> Atom.to_string(dep)
          dep -> dep
        end)

      {:ok, normalized_deps}
    else
      {:error, :invalid_step_deps}
    end
  end

  defp validate_optional_deps(repo, deps) do
    invalid_dep = Enum.find(deps, fn dep -> not is_binary(dep) or dep == "" end)

    case invalid_dep do
      nil -> validate_known_deps(repo, deps)
      invalid_dep -> {:error, {:invalid_step_dep, invalid_dep}}
    end
  end

  defp validate_known_deps(repo, deps) do
    case Enum.find_value(deps, &invalid_known_dep(repo, &1)) do
      nil -> :ok
      reason -> {:error, reason}
    end
  end

  defp invalid_known_dep(repo, dep) do
    case Flows.valid_slug?(repo, dep) do
      {:ok, true} -> nil
      {:ok, false} -> {:invalid_step_dep_slug, dep}
      {:error, reason} -> {:slug_validation_failed, dep, reason}
    end
  end

  defp normalize_step_type(step) do
    step_type = get_step_value(step, :step_type, "single")

    normalized_step_type =
      case step_type do
        type when is_atom(type) -> Atom.to_string(type)
        type -> type
      end

    if normalized_step_type in @allowed_step_types do
      {:ok, normalized_step_type}
    else
      {:error, {:invalid_step_type, normalized_step_type}}
    end
  end

  # Carries `if`/`if_not`/`when_unmet`/`when_exhausted` through to the step
  # map, mirroring the compile-time validation rules in
  # `PgFlow.DSL.Validation.validate_step_opts!/2`. Only present keys are
  # included in the result, so steps without conditions keep hitting the
  # cheaper 8-arg `add_step` path in `PgFlow.Queries.Flows`.
  defp normalize_step_conditions(step, slug) do
    has_if = step_has_key?(step, :if)
    has_if_not = step_has_key?(step, :if_not)
    has_when_unmet = step_has_key?(step, :when_unmet)

    with {:ok, if_opts} <- normalize_condition_pattern(step, :if, has_if),
         {:ok, if_not_opts} <- normalize_condition_pattern(step, :if_not, has_if_not),
         :ok <- validate_when_unmet_requires_condition(has_when_unmet, has_if, has_if_not, slug),
         {:ok, when_unmet_opts} <- normalize_condition_mode(step, :when_unmet),
         {:ok, when_exhausted_opts} <- normalize_condition_mode(step, :when_exhausted) do
      {:ok,
       if_opts
       |> Map.merge(if_not_opts)
       |> Map.merge(when_unmet_opts)
       |> Map.merge(when_exhausted_opts)}
    end
  end

  defp step_has_key?(step, key) do
    Map.has_key?(step, key) or Map.has_key?(step, Atom.to_string(key))
  end

  defp normalize_condition_pattern(_step, _key, false), do: {:ok, %{}}

  defp normalize_condition_pattern(step, key, true) do
    value = get_step_value(step, key)

    if is_map(value) do
      {:ok, %{Atom.to_string(key) => value}}
    else
      {:error, {:invalid_condition_pattern, key, value}}
    end
  end

  defp validate_when_unmet_requires_condition(true, false, false, slug),
    do: {:error, {:when_unmet_requires_condition, slug}}

  defp validate_when_unmet_requires_condition(_has_when_unmet, _has_if, _has_if_not, _slug),
    do: :ok

  defp normalize_condition_mode(step, key) do
    if step_has_key?(step, key) do
      value = get_step_value(step, key)

      if value in @skip_modes or value in @skip_mode_strings do
        {:ok, %{Atom.to_string(key) => value}}
      else
        {:error, {:invalid_condition_mode, key, value}}
      end
    else
      {:ok, %{}}
    end
  end

  defp validate_step_dependencies(step_maps) do
    slugs = MapSet.new(step_maps, fn step -> step["slug"] end)

    case Enum.find_value(step_maps, &unknown_dependency(&1, slugs)) do
      nil -> :ok
      reason -> {:error, reason}
    end
  end

  defp unknown_dependency(step, slugs) do
    step_slug = step["slug"]

    Enum.find_value(step["deps"], fn dep_slug ->
      if MapSet.member?(slugs, dep_slug),
        do: nil,
        else: {:unknown_dependency, step_slug, dep_slug}
    end)
  end

  defp get_step_value(step, key, default \\ nil) do
    case Map.fetch(step, key) do
      {:ok, value} -> value
      :error -> Map.get(step, to_string(key), default)
    end
  end

  defp wait_for_completion(repo, run_id, timeout, poll_interval) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_loop(repo, run_id, deadline, poll_interval)
  end

  defp wait_loop(repo, run_id, deadline, poll_interval) do
    case repo.get(Run, run_id) do
      nil ->
        {:error, :not_found}

      %Run{status: "completed"} = run ->
        {:ok, run}

      %Run{status: "failed"} = run ->
        {:error, run}

      %Run{} ->
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          {:error, :timeout}
        else
          Process.sleep(poll_interval)
          wait_loop(repo, run_id, deadline, poll_interval)
        end
    end
  end
end
