defmodule PgFlow.Queries.Pgmq do
  @moduledoc """
  SQL query interface for pgmq-specific operations.

  Provides functions for managing pgmq NOTIFY triggers and querying
  pgmq extension metadata.
  """

  alias Ecto.Adapters.SQL

  require Logger

  @notify_trigger "trigger_notify_queue_insert_listeners"
  @default_lock_timeout_ms 2_000

  # Reports whether the queue's notify trigger is the deferred one pgflow
  # installs (deferrable, initially deferred, calling
  # `pgmq.notify_queue_listeners` with the queue name as its only argument)
  # and the queue's throttle interval, or NULL when it has no throttle row.
  # Reads catalogs and the throttle table only, so it takes no lock that
  # conflicts with producers or readers.
  @notify_state_sql """
  SELECT
    EXISTS (
      SELECT 1
      FROM pg_trigger t
      WHERE t.tgrelid = to_regclass(format('pgmq.%I', pgmq.format_table_name($1::text, 'q'::text)))
        AND t.tgname = $2::text
        AND t.tgdeferrable
        AND t.tginitdeferred
        AND t.tgfoid = to_regprocedure('pgmq.notify_queue_listeners()')
        AND t.tgnargs = 1
        AND t.tgargs = convert_to($1::text, getdatabaseencoding()) || '\\x00'::bytea
    ),
    (SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle WHERE queue_name = $1::text)
  """

  # Serializes installers of one queue's trigger across nodes for the rest of
  # the transaction.
  @install_lock_sql "SELECT pg_advisory_xact_lock(hashtextextended('pgflow:notify:' || $1::text, 0))"

  # Restores or reconciles one queue's throttle row without touching the
  # trigger. An existing row keeps its `last_notified_at`; a restored row
  # starts at pgmq's column default.
  @upsert_throttle_sql """
  INSERT INTO pgmq.notify_insert_throttle (queue_name, throttle_interval_ms)
  VALUES ($1::text, $2::integer)
  ON CONFLICT (queue_name) DO UPDATE
    SET throttle_interval_ms = EXCLUDED.throttle_interval_ms
    WHERE pgmq.notify_insert_throttle.throttle_interval_ms IS DISTINCT FROM EXCLUDED.throttle_interval_ms
  """

  # Inserts a throttle row for each listed queue that exists in `pgmq.meta`,
  # has a notify trigger (deferred or immediate), and has no throttle row.
  # Returns the restored queue names. Reads catalogs and writes only the
  # throttle table, so it takes no lock on any queue table.
  @restore_throttle_rows_sql """
  INSERT INTO pgmq.notify_insert_throttle (queue_name, throttle_interval_ms)
  SELECT m.queue_name, $2::integer
  FROM pgmq.meta m
  WHERE m.queue_name = ANY ($1::text[])
    AND EXISTS (
      SELECT 1
      FROM pg_trigger t
      WHERE t.tgrelid = to_regclass(format('pgmq.%I', pgmq.format_table_name(m.queue_name, 'q'::text)))
        AND t.tgname = $3::text
    )
    AND NOT EXISTS (
      SELECT 1 FROM pgmq.notify_insert_throttle n WHERE n.queue_name = m.queue_name
    )
  ON CONFLICT (queue_name) DO NOTHING
  RETURNING queue_name
  """

  # Fires the transaction's pending deferred notify trigger events now when
  # any `pgmq.trigger_notify_queue_insert_listeners` constraint exists, so a
  # later DROP, TRUNCATE, or ALTER of a queue table in the same transaction
  # does not fail with "pending trigger events". `SET CONSTRAINTS` on a name
  # that matches no constraint raises, hence the catalog check. The setting
  # lasts until the transaction ends.
  @flush_notify_events_sql """
  DO $flush$
  BEGIN
    IF EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conname = '#{@notify_trigger}'
        AND connamespace = to_regnamespace('pgmq')
    ) THEN
      SET CONSTRAINTS pgmq.#{@notify_trigger} IMMEDIATE;
    END IF;
  END
  $flush$
  """

  @savepoint_sql "SAVEPOINT pgflow_notify_deferred_swap"
  @release_savepoint_sql "RELEASE SAVEPOINT pgflow_notify_deferred_swap"
  @rollback_savepoint_sql "ROLLBACK TO SAVEPOINT pgflow_notify_deferred_swap"

  # Builds the DDL that replaces pgmq's insert trigger with an initially
  # deferred one. The queue table comes from `pgmq.format_table_name/2`, the
  # same derivation `pgmq.create/1` and `pgmq.disable_notify_insert/1` use;
  # `%I` quotes it as an identifier and `%L` quotes the queue name as the
  # trigger argument. pgmq releases whose `notify_queue_listeners()` reads
  # `TG_ARGV[0]` take the queue name from that argument; releases that derive
  # it from `TG_TABLE_NAME` ignore the argument.
  @deferred_trigger_ddl_sql """
  SELECT
    format('DROP TRIGGER IF EXISTS %1$I ON pgmq.%2$I', $2::text, queue.queue_table),
    format(
      'CREATE CONSTRAINT TRIGGER %1$I AFTER INSERT ON pgmq.%2$I '
      'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
      'EXECUTE FUNCTION pgmq.notify_queue_listeners(%3$L)',
      $2::text,
      queue.queue_table,
      $1::text
    )
  FROM (SELECT pgmq.format_table_name($1::text, 'q'::text) AS queue_table) AS queue
  """

  @typedoc "Which INSERT trigger `enable_notify_insert/4` leaves on the queue table."
  @type notify_trigger :: :deferred | :pgmq_default

  @typedoc "Options accepted by `enable_notify_insert/4`."
  @type enable_option :: {:trigger, notify_trigger()} | {:lock_timeout, pos_integer()}

  @doc """
  Enables NOTIFY triggers for a pgmq queue.

  `throttle_ms` is the minimum interval between notifications for the queue.

  ## Options

    * `:trigger` - `:deferred` (default) or `:pgmq_default`.
    * `:lock_timeout` - milliseconds the `:deferred` install waits for the
      queue table lock before failing. Default: `#{@default_lock_timeout_ms}`.

  ## `:deferred`

  pgmq creates its INSERT trigger `INITIALLY IMMEDIATE`, so each insert
  updates the queue's `pgmq.notify_insert_throttle` row and holds that row
  lock until the inserting transaction ends: one long transaction that
  enqueues blocks every other producer on the queue. This mode leaves the
  trigger `DEFERRABLE INITIALLY DEFERRED`, so it runs at COMMIT, the row lock
  lasts only for the commit itself, and a rolled-back transaction sends no
  notification.

  When the queue already has the deferred trigger, the call changes nothing but
  the queue's throttle row, with no DDL and no queue table lock: it updates the
  row's throttle interval when it differs, and inserts the row when it is
  missing. pgmq keeps `pgmq.notify_insert_throttle` UNLOGGED, so crash
  recovery empties it, and so does Neon whenever a compute restarts; while a
  queue's row is missing, its trigger sends no NOTIFY. `PgFlow.FlowStarter`
  calls this on every boot, so steady-state boots take this path.

  Otherwise it runs one transaction that sets `lock_timeout` to the
  `:lock_timeout` option, takes a per-queue advisory lock so installers on
  other nodes wait for each other, re-checks the trigger under that lock, and
  then calls `pgmq.enable_notify_insert/2` and recreates its trigger under the
  same name as `DEFERRABLE INITIALLY DEFERRED`. pgmq's function drops and
  recreates the trigger, which takes an ACCESS EXCLUSIVE lock on the queue
  table; when an open transaction holds a conflicting lock for longer than
  `:lock_timeout`, the call returns the `lock_not_available` `Postgrex.Error`
  instead of making every later send and read on the queue wait behind it.

  If pgmq's function succeeds but recreating the trigger deferred fails (for
  example on missing privileges or a renamed pgmq object), only that swap is
  rolled back: pgmq's immediate trigger stays, so notifications still work,
  a warning is logged, and the call returns `:ok`.
  `PgFlow.SchemaCheck.warnings/2` reports the immediate trigger.

  ## `:pgmq_default`

  Calls only `pgmq.enable_notify_insert/2`, which leaves pgmq's immediate
  trigger, in a single statement with no `lock_timeout`.

  ## Transactions

  Returns `{:error, :in_transaction}` when called inside a transaction on
  `repo`. Run there, pgmq's DROP TRIGGER would hold ACCESS EXCLUSIVE on the
  queue table until the caller commits, and dropping a deferred trigger after
  the caller's own send in that transaction silently discards that send's
  pending notification. `PgFlow.FlowStarter` never calls it inside a
  transaction.
  """
  @spec enable_notify_insert(module(), String.t(), non_neg_integer(), [enable_option()]) ::
          :ok | {:error, :in_transaction | term()}
  def enable_notify_insert(repo, queue_name, throttle_ms, opts \\ []) do
    if repo.in_transaction?() do
      {:error, :in_transaction}
    else
      trigger = Keyword.get(opts, :trigger, :deferred)
      lock_timeout = Keyword.get(opts, :lock_timeout, @default_lock_timeout_ms)
      enable(repo, queue_name, throttle_ms, trigger, lock_timeout)
    end
  end

  defp enable(repo, queue_name, throttle_ms, :pgmq_default, _lock_timeout) do
    run_pgmq_enable(repo, queue_name, throttle_ms)
  rescue
    error in DBConnection.ConnectionError -> {:error, error}
  end

  defp enable(repo, queue_name, throttle_ms, :deferred, lock_timeout) do
    case notify_state(repo, queue_name) do
      {:ok, {true, ^throttle_ms}} ->
        :ok

      {:ok, {true, _current_ms}} ->
        transaction_with_lock_timeout(repo, lock_timeout, fn ->
          upsert_throttle(repo, queue_name, throttle_ms)
        end)

      {:ok, _state} ->
        transaction_with_lock_timeout(repo, lock_timeout, fn ->
          install_deferred_under_lock(repo, queue_name, throttle_ms)
        end)

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    error in DBConnection.ConnectionError -> {:error, error}
  end

  # Runs `fun` in a transaction whose lock waits are bounded by
  # `lock_timeout` milliseconds. `fun` returns `:ok` or `{:error, reason}`.
  defp transaction_with_lock_timeout(repo, lock_timeout, fun) do
    fn ->
      with {:ok, _} <-
             SQL.query(repo, "SELECT set_config('lock_timeout', $1::text, true)", [
               "#{lock_timeout}ms"
             ]),
           :ok <- fun.() do
        :ok
      else
        {:error, reason} -> repo.rollback(reason)
      end
    end
    |> repo.transaction()
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] -> {:error, error}
  end

  # Holds the per-queue install lock, then re-reads the trigger state, since
  # another node may have finished the install while this one waited.
  defp install_deferred_under_lock(repo, queue_name, throttle_ms) do
    with {:ok, _} <- SQL.query(repo, @install_lock_sql, [queue_name]),
         {:ok, state} <- notify_state(repo, queue_name) do
      install_or_reconcile(repo, queue_name, throttle_ms, state)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp install_or_reconcile(_repo, _queue_name, throttle_ms, {true, throttle_ms}), do: :ok

  defp install_or_reconcile(repo, queue_name, throttle_ms, {true, _current_ms}),
    do: upsert_throttle(repo, queue_name, throttle_ms)

  defp install_or_reconcile(repo, queue_name, throttle_ms, _state) do
    with :ok <- run_pgmq_enable(repo, queue_name, throttle_ms) do
      swap_to_deferred(repo, queue_name)
    end
  end

  # Replaces pgmq's immediate trigger with the deferred one inside a
  # savepoint. On failure it rolls back to the savepoint, which keeps pgmq's
  # immediate trigger, logs a warning, and returns `:ok`.
  defp swap_to_deferred(repo, queue_name) do
    with {:ok, _} <- SQL.query(repo, @savepoint_sql, []) do
      case replace_with_deferred_trigger(repo, queue_name) do
        :ok -> release_savepoint(repo)
        {:error, reason} -> keep_immediate_trigger(repo, queue_name, reason)
      end
    end
  end

  defp replace_with_deferred_trigger(repo, queue_name) do
    with {:ok, %{rows: [[drop_sql, create_sql]]}} <-
           SQL.query(repo, @deferred_trigger_ddl_sql, [queue_name, @notify_trigger]),
         {:ok, _} <- SQL.query(repo, drop_sql, []),
         {:ok, _} <- SQL.query(repo, create_sql, []) do
      :ok
    else
      {:ok, unexpected} -> {:error, {:unexpected_ddl_result, unexpected}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp release_savepoint(repo) do
    case SQL.query(repo, @release_savepoint_sql, []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp keep_immediate_trigger(repo, queue_name, reason) do
    case SQL.query(repo, @rollback_savepoint_sql, []) do
      {:ok, _} ->
        Logger.warning(
          "[PgFlow] kept pgmq's immediate notify trigger on queue #{inspect(queue_name)}; " <>
            "recreating it DEFERRABLE INITIALLY DEFERRED failed: #{inspect(reason)}"
        )

        :ok

      {:error, rollback_reason} ->
        {:error, rollback_reason}
    end
  end

  defp run_pgmq_enable(repo, queue_name, throttle_ms) do
    case SQL.query(repo, "SELECT pgmq.enable_notify_insert($1::text, $2::integer)", [
           queue_name,
           throttle_ms
         ]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp upsert_throttle(repo, queue_name, throttle_ms) do
    case SQL.query(repo, @upsert_throttle_sql, [queue_name, throttle_ms]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp notify_state(repo, queue_name) do
    case SQL.query(repo, @notify_state_sql, [queue_name, @notify_trigger]) do
      {:ok, %{rows: [[deferred?, throttle_ms]]}} -> {:ok, {deferred?, throttle_ms}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Inserts the missing `pgmq.notify_insert_throttle` row of each queue in
  `queue_names` that has a notify trigger, with `throttle_ms` as its interval,
  and returns the names of the queues it restored.

  pgmq keeps the throttle table UNLOGGED, so crash recovery empties it, and so
  does Neon whenever a compute restarts; a trigger whose row is missing sends
  no NOTIFY. This runs one statement that changes no trigger and locks no
  queue table. It skips queues that already have a row, have no notify
  trigger, or do not exist. `PgFlow.Signal.Notify` runs it periodically for
  the queues its workers listen on.
  """
  @spec restore_notify_throttle_rows(module(), [String.t()], non_neg_integer()) ::
          {:ok, [String.t()]} | {:error, term()}
  def restore_notify_throttle_rows(_repo, [], _throttle_ms), do: {:ok, []}

  def restore_notify_throttle_rows(repo, queue_names, throttle_ms) do
    case SQL.query(repo, @restore_throttle_rows_sql, [queue_names, throttle_ms, @notify_trigger]) do
      {:ok, %{rows: rows}} -> {:ok, List.flatten(rows)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    error in DBConnection.ConnectionError -> {:error, error}
  end

  @doc """
  Fires the current transaction's pending deferred notify trigger events.

  A deferred trigger keeps one pending event per inserted row until COMMIT,
  and Postgres refuses to DROP, TRUNCATE, or ALTER a table that has pending
  trigger events. `pgmq.drop_queue/1` or `pgmq.purge_queue/1` after a send in
  the same transaction therefore fails with "cannot DROP TABLE ... because it
  has pending trigger events". Call this earlier in that transaction.

  It runs `SET CONSTRAINTS pgmq.trigger_notify_queue_insert_listeners
  IMMEDIATE` only when a constraint by that name exists, so it does nothing on
  pgmq releases without notify triggers. The immediate mode lasts until the
  transaction ends: later sends in that transaction notify at insert time and
  hold the throttle row lock until commit. Outside a transaction it has no
  lasting effect.
  """
  @spec flush_notify_events(module()) :: :ok | {:error, term()}
  def flush_notify_events(repo) do
    case SQL.query(repo, @flush_notify_events_sql, []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec notify_trigger_name() :: String.t()
  def notify_trigger_name, do: @notify_trigger

  @doc """
  Disables NOTIFY triggers for a pgmq queue.

  Calls `pgmq.disable_notify_insert/1` to remove the INSERT trigger
  that fires NOTIFY events when messages are added to the queue.
  """
  @spec disable_notify_insert(module(), String.t()) :: :ok | {:error, term()}
  def disable_notify_insert(repo, queue_name) do
    case SQL.query(repo, "SELECT pgmq.disable_notify_insert($1::text)", [queue_name]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Gets the installed pgmq version.

  Checks two sources in order:
  1. `pg_extension` catalog — works when pgmq is installed via `CREATE EXTENSION pgmq`
  2. Feature detection — checks for the `enable_notify_insert` function in the `pgmq` schema,
     which indicates pgmq >= 1.8.0 installed from a SQL dump (e.g., on Neon or other managed Postgres)

  Returns `{:ok, version}` if pgmq is detected, or `{:error, :not_installed}` if not.
  """
  @spec get_pgmq_version(module()) :: {:ok, String.t()} | {:error, :not_installed | term()}
  def get_pgmq_version(repo) do
    case get_extension_version(repo) do
      {:ok, version} -> {:ok, version}
      {:error, :not_installed} -> detect_version_by_features(repo)
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_extension_version(repo) do
    case SQL.query(repo, "SELECT extversion FROM pg_extension WHERE extname = 'pgmq'", []) do
      {:ok, %{rows: [[version]]}} -> {:ok, version}
      {:ok, %{rows: []}} -> {:error, :not_installed}
      {:error, reason} -> {:error, reason}
    end
  end

  defp detect_version_by_features(repo) do
    case SQL.query(
           repo,
           """
           SELECT EXISTS(
             SELECT 1 FROM pg_proc
             WHERE proname = 'enable_notify_insert'
             AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')
           )
           """,
           []
         ) do
      {:ok, %{rows: [[true]]}} -> {:ok, "1.8.0"}
      {:ok, %{rows: [[false]]}} -> {:error, :not_installed}
      {:error, reason} -> {:error, reason}
    end
  end
end
