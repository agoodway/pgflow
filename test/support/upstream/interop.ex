defmodule PgFlow.Test.UpstreamInteropFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :interop_single_step, max_attempts: 3

  step :work do
    fn input, _ctx ->
      %{
        producer: input["producer"],
        value: (input["value"] || 0) + 1
      }
    end
  end
end

defmodule PgFlow.Test.MixedLangFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :mixed_lang_flow

  step :prepare do
    fn _input, _ctx ->
      Process.sleep(50)
      %{ok: true}
    end
  end

  step :process, depends_on: [:prepare] do
    fn _input, _ctx ->
      Process.sleep(50)
      %{ok: true}
    end
  end

  step :deliver, depends_on: [:process] do
    fn _input, _ctx ->
      Process.sleep(50)
      %{ok: true}
    end
  end
end

defmodule PgFlow.Test.UpstreamInterop.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :pgflow, adapter: Ecto.Adapters.Postgres
end

defmodule PgFlow.Test.UpstreamInterop do
  @moduledoc false

  alias PgFlow.Flow.Shape
  alias PgFlow.Test.MixedLangFlow
  alias PgFlow.Test.UpstreamHarness
  alias PgFlow.Test.UpstreamInterop.Repo
  alias PgFlow.Test.UpstreamInteropFlow
  alias PgFlow.Test.WaitHelpers
  alias PgFlow.Worker.Server, as: WorkerServer

  @mixed_run_count 20

  @script_dir Path.expand(".", Path.dirname(__ENV__.file))

  @doc "Runs both producer/worker language directions against a compatibility database."
  @spec run_reverse_interop!(Path.t(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def run_reverse_interop!(checkout, database_url) do
    slug = UpstreamHarness.manifest()["harness"]["interop_flow_slug"]

    try do
      with :ok <- ensure_interop_flow!(database_url, slug),
           {:ok, elixir_start} <- run_elixir_start_ts_worker!(checkout, database_url, slug),
           {:ok, ts_start} <- run_ts_start_elixir_worker!(checkout, database_url, slug) do
        {:ok,
         %{
           tests: 2,
           failures: 0,
           status: :passed,
           details: %{
             elixir_start_ts_worker: elixir_start,
             ts_start_elixir_worker: ts_start
           }
         }}
      else
        {:error, reason} ->
          {:ok,
           %{
             tests: 2,
             failures: 1,
             status: :failed,
             details: %{"error" => reason}
           }}
      end
    rescue
      exception ->
        {:ok,
         %{
           tests: 2,
           failures: 1,
           status: :failed,
           details: %{"error" => Exception.message(exception)}
         }}
    end
  end

  @doc """
  Compiles one three-step flow from TypeScript and from Elixir, then runs both
  workers on its flow-mode queue.
  """
  @spec run_mixed_workers!(Path.t(), String.t()) :: {:ok, map()} | {:error, String.t()}
  def run_mixed_workers!(checkout, database_url) do
    slug = Atom.to_string(MixedLangFlow.__pgflow_slug__())
    shape = Shape.from_definition(MixedLangFlow.__pgflow_definition__())

    with {:ok, ts_compile} <-
           run_worker_scenario!(checkout, database_url, "compile_mixed_flow", %{}),
         {:ok, elixir_status} <- compile_elixir_shape!(database_url, slug, shape) do
      share_mixed_runs!(checkout, database_url, slug, ts_compile["status"], elixir_status)
    end
  end

  @doc "Starts an isolated Elixir worker and waits for the supplied run to finish."
  @spec run_elixir_worker_for_run!(String.t(), String.t()) :: :ok | {:error, String.t()}
  def run_elixir_worker_for_run!(database_url, run_id) do
    run_elixir_worker!(database_url, run_id)
  end

  @doc "Installs the single-step flow shared by the interoperability scenarios."
  @spec ensure_interop_flow!(String.t(), String.t()) :: :ok | {:error, String.t()}
  def ensure_interop_flow!(database_url, slug) do
    with {:ok, conn} <- connect(database_url) do
      result =
        with :ok <- exec!(conn, "SELECT pgflow.create_flow($1)", [slug]) do
          exec!(conn, "SELECT pgflow.add_step($1, 'work')", [slug])
        end

      GenServer.stop(conn)
      result
    end
  end

  defp run_elixir_start_ts_worker!(checkout, database_url, slug) do
    with {:ok, conn} <- connect(database_url),
         {:ok, run_id} <- start_flow!(conn, slug, %{"producer" => "elixir", "value" => 2}),
         {:ok, worker_result} <-
           run_worker_scenario!(
             checkout,
             database_url,
             "worker_existing_flow",
             %{
               "PGFLOW_INTEROP_FLOW_SLUG" => slug,
               "PGFLOW_INTEROP_RUN_ID" => run_id
             }
           ),
         :ok <- assert_run_status!(conn, run_id, "completed"),
         :ok <- assert_step_output!(conn, run_id, %{"producer" => "elixir", "value" => 3}) do
      GenServer.stop(conn)

      {:ok,
       %{
         run_id: run_id,
         worker: worker_result
       }}
    end
  end

  defp run_ts_start_elixir_worker!(checkout, database_url, slug) do
    with {:ok, ts_result} <-
           run_worker_scenario!(
             checkout,
             database_url,
             "ts_start",
             %{"PGFLOW_INTEROP_FLOW_SLUG" => slug}
           ),
         run_id <- ts_result["run_id"],
         :ok <- run_elixir_worker!(database_url, run_id),
         {:ok, conn} <- connect(database_url) do
      try do
        case assert_step_output!(conn, run_id, %{"producer" => "typescript", "value" => 5}) do
          :ok -> {:ok, %{run_id: run_id, ts_start: ts_result}}
          {:error, _} = error -> error
        end
      after
        GenServer.stop(conn)
      end
    end
  end

  defp run_elixir_worker!(database_url, run_id) do
    with_started_test_repo!(database_url, fn repo_name ->
      run_elixir_worker_with_repo!(repo_name, run_id)
    end)
  end

  defp run_elixir_worker_with_repo!(repo_module, run_id) do
    {:ok, task_supervisor} = Task.Supervisor.start_link()

    try do
      config = %{
        flow_module: UpstreamInteropFlow,
        repo: repo_module,
        task_supervisor: task_supervisor,
        max_concurrency: 1,
        batch_size: 5,
        signal_strategy: :polling,
        min_poll_interval: 50,
        max_poll_interval: 500,
        notify_fallback_interval: 30_000,
        heartbeat_interval: 10_000
      }

      {:ok, worker_pid} = WorkerServer.start_link(config)

      try do
        WaitHelpers.wait_for_run_completion(repo_module, run_id, timeout: 20_000)
        :ok
      after
        if Process.alive?(worker_pid), do: GenServer.stop(worker_pid, :normal, 5_000)
      end
    after
      if Process.alive?(task_supervisor), do: Supervisor.stop(task_supervisor)
    end
  end

  defp with_started_test_repo!(database_url, fun) do
    {:ok, repo_pid} =
      Repo.start_link(url: database_url, pool: DBConnection.ConnectionPool, pool_size: 4)

    try do
      fun.(Repo)
    after
      Supervisor.stop(repo_pid)
    end
  end

  defp run_worker_scenario!(checkout, database_url, scenario, extra_env) do
    script = Path.join(@script_dir, "worker.mjs")

    env =
      [
        {"PGFLOW_UPSTREAM_CHECKOUT", checkout},
        {"DATABASE_URL", database_url},
        {"PGFLOW_COMPAT_DATABASE_URL", database_url}
      ] ++ Enum.map(extra_env, fn {k, v} -> {k, v} end)

    case System.cmd(node_cmd(), [script, "--scenario", scenario],
           cd: @script_dir,
           env: env,
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        case decode_worker_json(output) do
          {:ok, %{"failures" => 0, "results" => [result | _]}} ->
            {:ok, result["details"]}

          {:ok, decoded} ->
            {:error, "worker scenario #{scenario} failed: #{inspect(decoded)}"}

          _ ->
            {:error, "invalid JSON from worker.mjs: #{String.slice(output, 0, 400)}"}
        end

      {output, status} ->
        {:error, "worker.mjs #{scenario} exited #{status}: #{script_output_excerpt(output)}"}
    end
  end

  # Claim logs go to stderr. Keep the JSON object even if a line is mixed into stdout.
  defp decode_worker_json(output) do
    case extract_json_line(output) do
      nil -> :error
      json -> Jason.decode(json)
    end
  end

  defp extract_json_line(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find(&String.starts_with?(&1, "{"))
  end

  defp script_output_excerpt(output) do
    output = String.trim(output)
    if String.length(output) <= 800, do: output, else: String.slice(output, -800, 800)
  end

  defp start_flow!(conn, slug, input) do
    case Postgrex.query(conn, "SELECT run_id::text FROM pgflow.start_flow($1, $2::jsonb)", [
           slug,
           input
         ]) do
      {:ok, %{rows: [[run_id]]}} -> {:ok, run_id}
      {:error, error} -> {:error, Exception.message(error)}
    end
  end

  defp assert_step_output!(conn, run_id, expected) do
    case Postgrex.query(
           conn,
           "SELECT output FROM pgflow.step_tasks WHERE run_id = $1::text::uuid",
           [run_id]
         ) do
      {:ok, %{rows: [[^expected]]}} ->
        :ok

      {:ok, %{rows: rows}} ->
        {:error, "unexpected interop output #{inspect(rows)}; expected #{inspect(expected)}"}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  defp assert_run_status!(conn, run_id, expected) do
    case Postgrex.query(conn, "SELECT status FROM pgflow.runs WHERE run_id = $1::text::uuid", [
           run_id
         ]) do
      {:ok, %{rows: [[^expected]]}} ->
        :ok

      {:ok, %{rows: [[status]]}} ->
        {:error, "run #{run_id} status #{status}, expected #{expected}"}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  defp share_mixed_runs!(checkout, database_url, slug, ts_status, elixir_status) do
    ts_task =
      Task.async(fn ->
        run_worker_scenario!(
          checkout,
          database_url,
          "mixed_worker",
          %{"PGFLOW_MIXED_RUN_COUNT" => Integer.to_string(@mixed_run_count)}
        )
      end)

    result =
      with :ok <- wait_for_worker!(database_url, slug, "worker_#{slug}", ts_task),
           {:ok, elixir} <- start_mixed_elixir_worker!(database_url) do
        try do
          finish_mixed_runs!(database_url, slug, ts_status, elixir_status, ts_task)
        after
          stop_mixed_elixir_worker(elixir)
        end
      end

    shutdown_ts_task(ts_task)
    result
  end

  defp finish_mixed_runs!(database_url, slug, ts_status, elixir_status, ts_task) do
    elixir_function = "elixir:PgFlow.Test.MixedLangFlow"

    with :ok <- wait_for_worker!(database_url, slug, elixir_function, ts_task),
         :ok <- start_mixed_runs!(database_url, slug, @mixed_run_count),
         {:ok, completed} <- wait_for_completed_runs!(database_url, slug, @mixed_run_count),
         {:ok, stats} <- mixed_worker_stats!(database_url, slug) do
      {:ok,
       Map.merge(stats, %{
         ts_status: ts_status,
         elixir_status: elixir_status,
         completed_runs: completed
       })}
    end
  end

  defp compile_elixir_shape!(database_url, slug, shape) do
    with {:ok, conn} <- connect(database_url) do
      result =
        case Postgrex.query(
               conn,
               "SELECT pgflow.ensure_flow_compiled($1::text, $2::jsonb, 'flow', NULL::jsonb) AS result",
               [slug, shape]
             ) do
          {:ok, %{rows: [[%{"status" => status}]]}} -> {:ok, status}
          {:ok, %{rows: rows}} -> {:error, "unexpected compile result #{inspect(rows)}"}
          {:error, error} -> {:error, Exception.message(error)}
        end

      GenServer.stop(conn)
      result
    end
  end

  defp start_mixed_elixir_worker!(database_url) do
    with {:ok, repo_pid} <-
           Repo.start_link(url: database_url, pool: DBConnection.ConnectionPool, pool_size: 4),
         {:ok, task_supervisor} <- Task.Supervisor.start_link() do
      config = %{
        flow_module: MixedLangFlow,
        repo: Repo,
        task_supervisor: task_supervisor,
        max_concurrency: 1,
        batch_size: 1,
        signal_strategy: :polling,
        min_poll_interval: 50,
        max_poll_interval: 200,
        notify_fallback_interval: 30_000,
        heartbeat_interval: 10_000
      }

      case WorkerServer.start_link(config) do
        {:ok, worker_pid} ->
          {:ok, %{repo_pid: repo_pid, task_supervisor: task_supervisor, worker_pid: worker_pid}}

        {:error, reason} ->
          stop_supervisor(task_supervisor)
          stop_supervisor(repo_pid)
          {:error, "elixir worker failed to start: #{inspect(reason)}"}
      end
    end
  end

  defp stop_mixed_elixir_worker(%{
         repo_pid: repo_pid,
         task_supervisor: task_supervisor,
         worker_pid: worker_pid
       }) do
    stop_worker(worker_pid)
    stop_supervisor(task_supervisor)
    stop_supervisor(repo_pid)
  end

  defp stop_worker(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    else
      :ok
    end
  end

  defp stop_supervisor(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        Supervisor.stop(pid)
      catch
        :exit, _ -> :ok
      end
    else
      :ok
    end
  end

  defp start_mixed_runs!(database_url, slug, count) do
    with {:ok, conn} <- connect(database_url) do
      result = Enum.reduce_while(1..count, :ok, &start_one_mixed_run(conn, slug, &1, &2))
      GenServer.stop(conn)
      result
    end
  end

  defp start_one_mixed_run(conn, slug, n, :ok) do
    case start_flow!(conn, slug, %{"n" => n}) do
      {:ok, _run_id} -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp wait_for_completed_runs!(database_url, slug, expected) do
    deadline = System.monotonic_time(:millisecond) + 60_000

    with {:ok, conn} <- connect(database_url) do
      result = poll_completed_runs(conn, slug, expected, deadline)
      GenServer.stop(conn)
      result
    end
  end

  defp poll_completed_runs(conn, slug, expected, deadline) do
    case completed_run_count(conn, slug) do
      {:ok, count} when count >= expected ->
        {:ok, count}

      {:ok, count} ->
        wait_or_timeout(deadline, "timed out with #{count} completed runs", fn ->
          poll_completed_runs(conn, slug, expected, deadline)
        end)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp completed_run_count(conn, slug) do
    case Postgrex.query(
           conn,
           "SELECT count(*) FROM pgflow.runs WHERE flow_slug = $1 AND status = 'completed'",
           [slug]
         ) do
      {:ok, %{rows: [[count]]}} -> {:ok, count}
      {:error, error} -> {:error, Exception.message(error)}
    end
  end

  defp wait_for_worker!(database_url, queue, function_name, ts_task) do
    deadline = System.monotonic_time(:millisecond) + 20_000
    poll_worker(database_url, queue, function_name, ts_task, deadline)
  end

  defp poll_worker(database_url, queue, function_name, ts_task, deadline) do
    with :running <- ts_task_failure(ts_task),
         {:ok, false} <- worker_present?(database_url, queue, function_name) do
      wait_or_timeout(deadline, "timed out waiting for worker #{function_name}", fn ->
        poll_worker(database_url, queue, function_name, ts_task, deadline)
      end)
    else
      {:ok, true} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp wait_or_timeout(deadline, timeout_message, continue) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, timeout_message}
    else
      receive do
      after
        100 -> :ok
      end

      continue.()
    end
  end

  defp ts_task_failure(ts_task) do
    case Task.yield(ts_task, 0) do
      nil -> :running
      {:ok, {:ok, _details}} -> {:error, "typescript worker exited before the runs finished"}
      {:ok, {:error, reason}} -> {:error, "typescript worker failed: #{reason}"}
      {:ok, other} -> {:error, "typescript worker returned #{inspect(other)}"}
      {:exit, reason} -> {:error, "typescript worker exited: #{inspect(reason)}"}
    end
  end

  defp worker_present?(database_url, queue, function_name) do
    with {:ok, conn} <- connect(database_url) do
      result =
        case Postgrex.query(
               conn,
               """
               SELECT count(*) FROM pgflow.workers
               WHERE queue_name = $1 AND function_name = $2 AND stopped_at IS NULL
               """,
               [queue, function_name]
             ) do
          {:ok, %{rows: [[count]]}} -> {:ok, count > 0}
          {:error, error} -> {:error, Exception.message(error)}
        end

      GenServer.stop(conn)
      result
    end
  end

  defp mixed_worker_stats!(database_url, slug) do
    sql = """
    SELECT
      COALESCE(
        array_agg(DISTINCT task.last_worker_id::text)
          FILTER (WHERE task.last_worker_id IS NOT NULL),
        '{}'::text[]
      ),
      COALESCE(
        array_agg(DISTINCT worker.pgflow_version)
          FILTER (WHERE worker.pgflow_version IS NOT NULL),
        '{}'::text[]
      ),
      COALESCE(MAX(task.attempts_count), 0)
    FROM pgflow.step_tasks AS task
    LEFT JOIN pgflow.workers AS worker ON worker.queue_name = task.flow_slug
    WHERE task.flow_slug = $1
    """

    with {:ok, conn} <- connect(database_url) do
      result =
        case Postgrex.query(conn, sql, [slug]) do
          {:ok, %{rows: [[worker_ids, versions, max_attempts]]}} ->
            {:ok, %{worker_ids: worker_ids, versions: versions, max_attempts: max_attempts}}

          {:error, error} ->
            {:error, Exception.message(error)}
        end

      GenServer.stop(conn)
      result
    end
  end

  defp shutdown_ts_task(task) do
    case Task.shutdown(task, 5_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, _} -> :ok
      {:exit, _} -> :ok
      nil -> :ok
    end
  end

  defp connect(database_url) do
    Postgrex.start_link(postgrex_options(database_url))
  end

  defp exec!(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, _} -> :ok
      {:error, %Postgrex.Error{} = error} -> {:error, Exception.message(error)}
    end
  end

  defp node_cmd do
    System.get_env("PGFLOW_NODE") || System.find_executable("node") || "node"
  end

  defp postgrex_options(url) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo || "", ":", parts: 2)

    [
      hostname: uri.host,
      port: uri.port || 5432,
      username: URI.decode(username),
      password: URI.decode(password),
      database: uri.path |> String.trim_leading("/")
    ]
  end
end
