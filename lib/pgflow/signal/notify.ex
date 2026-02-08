defmodule PgFlow.Signal.Notify do
  @moduledoc """
  Manages PostgreSQL LISTEN/NOTIFY for pgmq queue notifications.

  Uses pgmq's built-in `enable_notify_insert` (pgmq 1.8.0+) to receive instant
  wake-up signals when messages are inserted into queue tables. Dispatches
  `:poll_now` messages to registered worker processes.

  Built on `Postgrex.Notifications` which is the purpose-built solution for
  PostgreSQL LISTEN/NOTIFY. Notifications are delivered asynchronously via
  messages to the process that called `listen/2`.
  """

  use GenServer

  require Logger

  alias PgFlow.Queries.Pgmq

  @min_pgmq_version "1.8.0"

  @type worker_entry :: %{
          worker_pid: pid(),
          monitor_ref: reference(),
          listen_ref: reference() | nil
        }

  @type state :: %{
          repo: module(),
          notify_throttle_ms: non_neg_integer(),
          conn: pid() | nil,
          workers: %{String.t() => worker_entry()},
          channels: %{String.t() => String.t()}
        }

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  # Client API

  @doc """
  Starts the Signal.Notify process.

  ## Options

    * `:repo` - (required) The Ecto repository module
    * `:notify_throttle_ms` - (optional) Throttle interval for pgmq notifications (default: 250)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers a worker for notifications on a queue.

  Starts listening on the queue's channel. Note that `pgmq.enable_notify_insert`
  must be called separately (done by PgFlow.Supervisor) before registration.
  """
  @spec register_worker(GenServer.server(), String.t(), pid()) :: :ok | {:error, term()}
  def register_worker(server \\ __MODULE__, flow_slug, worker_pid) do
    GenServer.call(server, {:register_worker, flow_slug, worker_pid})
  end

  @doc """
  Unregisters a worker from notifications.
  """
  @spec unregister_worker(GenServer.server(), String.t()) :: :ok
  def unregister_worker(server \\ __MODULE__, flow_slug) do
    GenServer.call(server, {:unregister_worker, flow_slug})
  end

  # GenServer Callbacks

  @impl GenServer
  def init(opts) do
    repo = Keyword.fetch!(opts, :repo)
    throttle_ms = Keyword.get(opts, :notify_throttle_ms, 250)

    verify_pgmq_version!(repo)

    # Start the Postgrex.Notifications connection
    conn_opts =
      repo
      |> repo_to_connection_opts()
      |> Keyword.put(:auto_reconnect, true)

    case Postgrex.Notifications.start_link(conn_opts) do
      {:ok, conn} ->
        state = %{
          repo: repo,
          notify_throttle_ms: throttle_ms,
          conn: conn,
          workers: %{},
          channels: %{}
        }

        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:register_worker, flow_slug, worker_pid}, _from, state) do
    channel = pgmq_channel(flow_slug)
    monitor_ref = Process.monitor(worker_pid)

    case Postgrex.Notifications.listen(state.conn, channel) do
      {status, listen_ref} when status in [:ok, :eventually] ->
        state = %{
          state
          | workers:
              Map.put(state.workers, flow_slug, %{
                worker_pid: worker_pid,
                monitor_ref: monitor_ref,
                listen_ref: listen_ref
              }),
            channels: Map.put(state.channels, channel, flow_slug)
        }

        {:reply, :ok, state}

      {:error, reason} ->
        Process.demonitor(monitor_ref, [:flush])
        Logger.error("Signal.Notify: failed to listen on #{channel}: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:unregister_worker, flow_slug}, _from, state) do
    case Map.pop(state.workers, flow_slug) do
      {%{monitor_ref: monitor_ref, listen_ref: listen_ref}, workers} ->
        Process.demonitor(monitor_ref, [:flush])
        maybe_unlisten(state.conn, listen_ref)
        disable_notify(state.repo, flow_slug)

        channel = pgmq_channel(flow_slug)
        state = %{state | workers: workers, channels: Map.delete(state.channels, channel)}
        {:reply, :ok, state}

      {nil, _workers} ->
        {:reply, :ok, state}
    end
  end

  @impl GenServer
  def handle_info({:notification, _conn_pid, _listen_ref, channel, _payload}, state) do
    with flow_slug when not is_nil(flow_slug) <- Map.get(state.channels, channel),
         %{worker_pid: pid} <- Map.get(state.workers, flow_slug) do
      send(pid, :poll_now)
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    case find_worker_by_pid(state.workers, pid) do
      {flow_slug, entry} ->
        maybe_unlisten(state.conn, entry.listen_ref)
        disable_notify(state.repo, flow_slug)

        channel = pgmq_channel(flow_slug)

        {:noreply,
         %{
           state
           | workers: Map.delete(state.workers, flow_slug),
             channels: Map.delete(state.channels, channel)
         }}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  # Private helpers

  defp find_worker_by_pid(workers, pid) do
    Enum.find(workers, fn {_slug, entry} -> entry.worker_pid == pid end)
  end

  defp pgmq_channel(flow_slug), do: "pgmq.q_#{flow_slug}.INSERT"

  defp maybe_unlisten(_conn, nil), do: :ok
  defp maybe_unlisten(conn, listen_ref), do: Postgrex.Notifications.unlisten(conn, listen_ref)

  defp disable_notify(repo, flow_slug) do
    case Pgmq.disable_notify_insert(repo, flow_slug) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to disable notify for #{flow_slug}: #{inspect(reason)}")
    end
  end

  defp repo_to_connection_opts(repo) do
    config = repo.config()

    # Parse URL if present (common in production configs)
    url_opts =
      case Keyword.get(config, :url) do
        url when is_binary(url) -> Ecto.Repo.Supervisor.parse_url(url)
        _ -> []
      end

    direct_opts =
      Keyword.take(config, [
        :hostname,
        :port,
        :database,
        :username,
        :password,
        :ssl,
        :ssl_opts,
        :socket_dir,
        :socket,
        :parameters
      ])

    # Direct config takes precedence over URL-parsed values
    # Drop pool opts — Postgrex.Notifications uses a single connection
    url_opts
    |> Keyword.merge(direct_opts)
    |> Keyword.drop([:pool, :pool_size])
  end

  defp verify_pgmq_version!(repo) do
    case check_pgmq_version(repo) do
      {:ok, _version} ->
        :ok

      {:error, :version_too_low, version} ->
        raise """
        PgFlow signal_strategy: :notify requires pgmq >= #{@min_pgmq_version}.
        Found: #{version}

        Either:
        1. Upgrade pgmq in your database to #{@min_pgmq_version} or later
        2. Use signal_strategy: :polling instead (no pgmq version requirement)

        To upgrade pgmq, run in psql:
          ALTER EXTENSION pgmq UPDATE TO '#{@min_pgmq_version}';

        Or update your docker-compose/database setup to use a newer pgmq version.
        """

      {:error, :not_installed} ->
        raise """
        PgFlow signal_strategy: :notify requires pgmq extension.
        The pgmq extension is not installed in the database.

        Install pgmq >= #{@min_pgmq_version} or use signal_strategy: :polling instead.
        """
    end
  end

  defp check_pgmq_version(repo) do
    case Pgmq.get_pgmq_version(repo) do
      {:ok, version} ->
        if version_gte?(version, @min_pgmq_version),
          do: {:ok, version},
          else: {:error, :version_too_low, version}

      {:error, :not_installed} ->
        {:error, :not_installed}

      {:error, reason} ->
        Logger.error("Failed to check pgmq version: #{inspect(reason)}")
        {:error, :not_installed}
    end
  end

  defp version_gte?(current, minimum) do
    with {:ok, current_ver} <- current |> normalize_version() |> Version.parse(),
         {:ok, min_ver} <- minimum |> normalize_version() |> Version.parse() do
      Version.compare(current_ver, min_ver) in [:gt, :eq]
    else
      _ -> current >= minimum
    end
  end

  defp normalize_version(version) do
    version
    |> String.trim()
    |> String.split(".")
    |> Enum.take(3)
    |> Enum.join(".")
  end
end
