defmodule PgFlow.SchemaCompatibility do
  @moduledoc "Checks that the configured repository has the helper objects required by this PgFlow release."

  @required_helper_version 5
  @default_initial_delay 100
  @default_max_delay 5_000
  @default_max_attempts 8

  @type compatibility_error :: String.t() | {:repo_unavailable, term()}

  @doc """
  Checks that `repo` has the V05 helpers required for await-signals.

  Verifies the helpers version and the required signal table/functions without
  changing the database. Returns `:ok` when the repository is compatible,
  `{:error, message}` when the schema is permanently incompatible, or
  `{:error, {:repo_unavailable, reason}}` when the check could not reach the
  repository. For an existing installation, generate and apply
  `mix pgflow.gen.helpers_migration --from-version 4` before starting the new
  worker release.
  """
  @spec check_await_signals(module()) :: :ok | {:error, compatibility_error()}
  def check_await_signals(repo) do
    case helper_version(repo) do
      {:ok, version} when version >= @required_helper_version ->
        check_required_objects(repo)

      {:ok, version} when version > 0 ->
        {:error,
         "PgFlow helpers V05 are required but the database is at #{format_version(version)}; " <>
           "run `mix pgflow.gen.helpers_migration --from-version #{version}` and apply the " <>
           "generated helpers upgrade migration"}

      {:error, reason} ->
        {:error, {:repo_unavailable, reason}}

      _missing_or_unreadable ->
        {:error,
         "PgFlow helper version is missing or unreadable; V05 is required. " <>
           "Determine the installed version, then run `mix pgflow.gen.helpers_migration " <>
           "--from-version VERSION` and apply the generated helpers upgrade migration before " <>
           "starting PgFlow"}
    end
  end

  @doc """
  Checks await-signals compatibility and raises if the repository is not ready.

  Returns `:ok` for a V05-compatible repository. Otherwise raises the upgrade
  guidance from `check_await_signals/1`; use `mix pgflow.gen.helpers_migration
  --from-version VERSION` to generate the version-aware migration before
  starting workers.
  """
  @spec check_await_signals!(module()) :: :ok
  def check_await_signals!(repo) do
    case check_await_signals(repo) do
      :ok -> :ok
      {:error, error} -> raise error_message(error)
    end
  end

  @doc """
  Formats a compatibility error for logs, exceptions, and operator-facing tools.
  """
  @spec error_message(compatibility_error()) :: String.t()
  def error_message({:repo_unavailable, reason}) do
    "PgFlow schema compatibility check could not reach the repository: #{inspect(reason)}"
  end

  def error_message(message) when is_binary(message), do: message

  @doc """
  Waits for transient repository availability before checking compatibility.

  Repository failures are retried with bounded exponential delays. An
  incompatible or unreadable helper schema fails immediately, and exhausting
  the availability attempts raises without starting PgFlow runtime children.

  By default, startup makes eight attempts. Retry delays start at 100 milliseconds,
  double after each failure, and are capped at 5 seconds. Tests and embedding
  applications may override `:initial_delay`, `:max_delay`, `:max_attempts`, and
  the `:sleep` callback.
  """
  @spec await_await_signals!(module(), keyword()) :: :ok
  def await_await_signals!(repo, opts \\ []) do
    initial_delay = Keyword.get(opts, :initial_delay, @default_initial_delay)
    max_delay = Keyword.get(opts, :max_delay, @default_max_delay)
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)
    sleep = Keyword.get(opts, :sleep, &Process.sleep/1)

    do_await_await_signals!(repo, 1, initial_delay, max_delay, max_attempts, sleep)
  end

  defp do_await_await_signals!(repo, attempt, delay, max_delay, max_attempts, sleep) do
    case check_await_signals(repo) do
      :ok ->
        :ok

      {:error, {:repo_unavailable, _reason}} when attempt < max_attempts ->
        sleep.(delay)

        do_await_await_signals!(
          repo,
          attempt + 1,
          min(delay * 2, max_delay),
          max_delay,
          max_attempts,
          sleep
        )

      {:error, {:repo_unavailable, reason}} ->
        raise "PgFlow could not verify schema compatibility because the repository is unavailable " <>
                "after #{attempt} attempts: #{inspect(reason)}"

      {:error, message} when is_binary(message) ->
        raise message
    end
  end

  defp helper_version(repo) do
    sql = """
    SELECT obj_description(c.oid)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'v'
    """

    case repo.query(sql, ["pgflow", "extensions_version"]) do
      {:ok, %{rows: [[comment]]}} when is_binary(comment) -> {:ok, parse_version(comment)}
      {:ok, _result} -> {:ok, 0}
      {:error, reason} -> {:error, reason}
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp parse_version(comment) do
    case Regex.run(~r/version=(\d+)/, comment) do
      [_, version] -> String.to_integer(version)
      _no_version -> 0
    end
  end

  defp check_required_objects(repo) do
    sql = """
    SELECT
      to_regclass('pgflow.task_signals') IS NOT NULL,
      to_regprocedure(
        'pgflow.await_task_signal(uuid,text,integer,integer,bigint,bigint,boolean)'
      ) IS NOT NULL,
      to_regprocedure('pgflow.signal_task(uuid,text,integer,jsonb)') IS NOT NULL,
      to_regprocedure('pgflow.expire_waiting_tasks(integer)') IS NOT NULL
    """

    case repo.query(sql) do
      {:ok, %{rows: [[true, true, true, true]]}} ->
        :ok

      {:ok, _result} ->
        {:error, "PgFlow helpers report V05 but await-signals objects are missing"}

      {:error, reason} ->
        {:error, {:repo_unavailable, reason}}
    end
  rescue
    error -> {:error, {:repo_unavailable, error}}
  catch
    kind, reason -> {:error, {:repo_unavailable, {kind, reason}}}
  end

  defp format_version(version) do
    "V" <> (version |> Integer.to_string() |> String.pad_leading(2, "0"))
  end
end
