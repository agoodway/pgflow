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

defmodule PgFlow.Test.UpstreamInterop.Repo do
  @moduledoc false
  use Ecto.Repo, otp_app: :pgflow, adapter: Ecto.Adapters.Postgres
end

defmodule PgFlow.Test.UpstreamInterop do
  @moduledoc false

  alias PgFlow.Test.UpstreamHarness
  alias PgFlow.Test.UpstreamInterop.Repo
  alias PgFlow.Test.UpstreamInteropFlow
  alias PgFlow.Test.WaitHelpers
  alias PgFlow.Worker.Server, as: WorkerServer

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
        case Jason.decode(output) do
          {:ok, %{"failures" => 0, "results" => [result | _]}} ->
            {:ok, result["details"]}

          {:ok, decoded} ->
            {:error, "worker scenario #{scenario} failed: #{inspect(decoded)}"}

          _ ->
            {:error, "invalid JSON from worker.mjs: #{String.slice(output, 0, 400)}"}
        end

      {output, status} ->
        {:error,
         "worker.mjs #{scenario} exited #{status}: #{String.slice(String.trim(output), 0, 500)}"}
    end
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
