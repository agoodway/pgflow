defmodule PgFlow.SchemaCheck do
  @moduledoc """
  Verifies that a database matches the pgflow schema contract expected by this
  library release.

  `mix pgflow.check_schema` checks installed core/helpers versions, function
  signatures, queue constraints, and the eight-field `step_task_record` layout.
  Worker startup uses `runtime_checks/1` for signatures, legacy claim absence,
  field names and types, and the helpers version floor before reserving messages.
  """

  alias Ecto.Adapters.SQL
  alias EctoEvolver.Adapters.Postgres

  @required_tables ~w(flows steps deps runs step_states step_tasks workers)
  @required_functions ~w(start_flow complete_task fail_task)

  @claim_signature "pgflow.start_tasks(text,bigint[],uuid,text)"
  @legacy_claim_signature "pgflow.start_tasks(text,bigint[],uuid)"
  @compile_signature "pgflow.ensure_flow_compiled(text,jsonb)"

  @record_fields ~w(flow_slug run_id step_slug input msg_id task_index flow_input attempts_count)
  @record_types ~w(text uuid text jsonb bigint integer jsonb integer)

  @doc """
  Checks the claim and startup protocol before any worker reserves messages.
  Connectivity failures retain their original error; incompatible schemas
  return `{:error, {:schema_incompatible, object}}`.
  """
  @spec runtime_checks(module()) :: :ok | {:error, term()}
  def runtime_checks(repo) do
    signatures = [
      ensure_flow_compiled: {@compile_signature, true},
      track_worker_function: {"pgflow.track_worker_function(text,text)", true},
      start_tasks: {@claim_signature, true},
      start_tasks_legacy: {@legacy_claim_signature, false}
    ]

    with :ok <- runtime_signatures(repo, signatures),
         :ok <- runtime_record_layout(repo) do
      runtime_helpers_version(repo)
    end
  end

  defp runtime_helpers_version(repo) do
    sql = """
    SELECT obj_description(c.oid)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'pgflow' AND c.relname = 'extensions_version' AND c.relkind = 'v'
    """

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[comment]]}} when is_binary(comment) ->
        validate_helpers_version(comment)

      {:ok, _} ->
        {:error, {:schema_incompatible, :helpers_version}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp validate_helpers_version(comment) do
    case Regex.run(~r/version=(\d+)/, comment, capture: :all_but_first) do
      [version] ->
        if String.to_integer(version) >= 5,
          do: :ok,
          else: {:error, {:schema_incompatible, :helpers_version}}

      _ ->
        {:error, {:schema_incompatible, :helpers_version}}
    end
  end

  defp runtime_signatures(repo, signatures) do
    Enum.reduce_while(signatures, :ok, fn {key, {signature, expected}}, :ok ->
      case SQL.query(repo, "SELECT to_regprocedure($1::text) IS NOT NULL", [signature]) do
        {:ok, %{rows: [[^expected]]}} -> {:cont, :ok}
        {:ok, _} -> {:halt, {:error, {:schema_incompatible, key}}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp runtime_record_layout(repo) do
    case record_layout(repo) do
      {:ok, %{rows: rows}} ->
        if rows == Enum.zip_with(@record_fields, @record_types, &[&1, &2]),
          do: :ok,
          else: {:error, {:schema_incompatible, :record_layout}}

      {:error, error} ->
        {:error, error}
    end
  end

  @task_statuses ~w(cancelled completed failed queued skipped started)

  @queue_tables ~w(step_tasks steps)

  @doc """
  Runs all schema compatibility checks against `repo`.

  Returns `:ok` when every check passes, or `{:error, messages}` with one
  human-readable message per failed check.
  """
  @spec run(module()) :: :ok | {:error, [String.t()]}
  def run(repo) do
    checks = [
      &check_schema_exists/1,
      &check_tables_exist/1,
      &check_core_functions_exist/1,
      &check_pgmq/1,
      &check_core_version/1,
      &check_helpers_version/1,
      &check_claim_signature/1,
      &check_legacy_claim_absent/1,
      &check_compile_signature/1,
      &check_record_layout/1,
      &check_task_statuses/1,
      &check_queue_constraints/1
    ]

    errors =
      Enum.flat_map(checks, fn check ->
        case check.(repo) do
          :ok -> []
          {:error, message} -> [message]
        end
      end)

    if errors == [], do: :ok, else: {:error, errors}
  end

  defp check_schema_exists(repo) do
    case repo_query(repo, """
         SELECT schema_name
         FROM information_schema.schemata
         WHERE schema_name = 'pgflow'
         """) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, "pgflow schema does not exist"}

      {:error, error} ->
        {:error, "failed to check pgflow schema: #{inspect(error)}"}
    end
  end

  defp check_tables_exist(repo) do
    case repo_query(repo, """
         SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'pgflow'
         """) do
      {:ok, %{rows: rows}} ->
        existing = Enum.map(rows, fn [name] -> name end)
        missing = @required_tables -- existing

        if missing == [] do
          :ok
        else
          {:error, "missing pgflow tables: #{Enum.join(missing, ", ")}"}
        end

      {:error, error} ->
        {:error, "failed to check pgflow tables: #{inspect(error)}"}
    end
  end

  defp check_core_functions_exist(repo) do
    case repo_query(repo, """
         SELECT routine_name
         FROM information_schema.routines
         WHERE routine_schema = 'pgflow'
           AND routine_type = 'FUNCTION'
         """) do
      {:ok, %{rows: rows}} ->
        existing = Enum.map(rows, fn [name] -> name end)
        missing = @required_functions -- existing

        if missing == [] do
          :ok
        else
          {:error, "missing pgflow functions: #{Enum.join(missing, ", ")}"}
        end

      {:error, error} ->
        {:error, "failed to check pgflow functions: #{inspect(error)}"}
    end
  end

  defp check_pgmq(repo) do
    extension_query = """
    SELECT extname
    FROM pg_extension
    WHERE extname = 'pgmq'
    """

    case repo_query(repo, extension_query) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        check_vendored_pgmq(repo)

      {:error, error} ->
        {:error, "failed to check pgmq extension: #{inspect(error)}"}
    end
  end

  defp check_vendored_pgmq(repo) do
    case repo_query(repo, """
         SELECT 1
         FROM information_schema.tables
         WHERE table_schema = 'pgmq' AND table_name = 'meta'
         """) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, %{num_rows: 0}} -> {:error, "pgmq extension is not installed"}
      {:error, error} -> {:error, "failed to check pgmq installation: #{inspect(error)}"}
    end
  end

  defp check_core_version(repo) do
    expected = PgFlow.Migration.current_version()

    case Postgres.get_version(repo, "pgflow", {:view, "pgflow_version"}) do
      version when version >= expected ->
        :ok

      0 ->
        {:error, "pgflow core is not installed (expected version #{expected})"}

      version ->
        {:error,
         "pgflow core version #{version} is below required version #{expected}; " <>
           "generate a new upgrade wrapper with `mix pgflow.setup --upgrade`"}
    end
  end

  defp check_helpers_version(repo) do
    expected = PgFlow.HelpersMigration.current_version()

    case Postgres.get_version(repo, "pgflow", {:view, "extensions_version"}) do
      version when version >= expected ->
        :ok

      0 ->
        {:error, "pgflow helpers are not installed (expected version #{expected})"}

      version ->
        {:error,
         "pgflow helpers version #{version} is below required version #{expected}; " <>
           "generate a new upgrade wrapper with `mix pgflow.setup --upgrade`"}
    end
  end

  defp check_claim_signature(repo) do
    if function_exists?(repo, @claim_signature) do
      :ok
    else
      {:error, "missing four-argument claim function #{@claim_signature}"}
    end
  end

  defp check_legacy_claim_absent(repo) do
    if function_exists?(repo, @legacy_claim_signature) do
      {:error,
       "obsolete three-argument claim #{@legacy_claim_signature} is still present; " <>
         "upgrade core and helpers before deploying this library release"}
    else
      :ok
    end
  end

  defp check_compile_signature(repo) do
    if function_exists?(repo, @compile_signature) do
      :ok
    else
      {:error, "missing startup compilation function #{@compile_signature}"}
    end
  end

  defp check_record_layout(repo) do
    case record_layout(repo) do
      {:ok, %{rows: rows}} ->
        fields = rows

        if fields == Enum.zip_with(@record_fields, @record_types, &[&1, &2]) do
          :ok
        else
          {:error,
           "step_task_record layout mismatch: expected #{inspect(@record_fields)}, got #{inspect(fields)}"}
        end

      {:error, %Postgrex.Error{postgres: %{code: :undefined_object}}} ->
        {:error, "pgflow.step_task_record composite type is missing"}

      {:error, error} ->
        {:error, "failed to inspect step_task_record layout: #{inspect(error)}"}
    end
  end

  defp record_layout(repo) do
    SQL.query(
      repo,
      """
      SELECT attname, format_type(atttypid, atttypmod)
      FROM pg_attribute
      WHERE attrelid = (SELECT typrelid FROM pg_type WHERE oid = to_regtype('pgflow.step_task_record'))
        AND attnum > 0 AND NOT attisdropped
      ORDER BY attnum
      """,
      []
    )
  end

  defp check_task_statuses(repo) do
    case repo_query(repo, """
         SELECT pg_get_constraintdef(pg_constraint.oid)
         FROM pg_constraint
         JOIN pg_class ON pg_class.oid = pg_constraint.conrelid
         JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
         WHERE pg_namespace.nspname = 'pgflow'
           AND pg_class.relname = 'step_tasks'
           AND pg_constraint.conname = 'valid_status'
         """) do
      {:ok, %{rows: [[definition]]}} ->
        missing =
          Enum.filter(@task_statuses, fn status ->
            not String.contains?(definition, "'#{status}'")
          end)

        if missing == [] do
          :ok
        else
          {:error, "step_tasks.valid_status check missing values: #{Enum.join(missing, ", ")}"}
        end

      {:ok, %{rows: []}} ->
        {:error, "pgflow.step_tasks.valid_status check constraint is missing"}

      {:error, error} ->
        {:error, "failed to inspect step_tasks.valid_status constraint: #{inspect(error)}"}
    end
  end

  defp check_queue_constraints(repo) do
    Enum.reduce_while(@queue_tables, :ok, fn table, :ok ->
      with :ok <- check_queue_column!(repo, table),
           :ok <- check_queue_check_constraint!(repo, table) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp check_queue_column!(repo, table) do
    case repo_query(
           repo,
           """
           SELECT is_nullable
           FROM information_schema.columns
           WHERE table_schema = 'pgflow'
             AND table_name = $1
             AND column_name = 'queue_name'
           """,
           [table]
         ) do
      {:ok, %{rows: [["NO"]]}} ->
        :ok

      {:ok, %{rows: []}} ->
        {:error, "pgflow.#{table}.queue_name column is missing"}

      {:ok, %{rows: _}} ->
        {:error, "pgflow.#{table}.queue_name must be NOT NULL after core V02"}

      {:error, error} ->
        {:error, "failed to inspect pgflow.#{table}.queue_name: #{inspect(error)}"}
    end
  end

  defp check_queue_check_constraint!(repo, table) do
    case repo_query(
           repo,
           """
           SELECT 1
           FROM pg_constraint
           JOIN pg_class ON pg_class.oid = pg_constraint.conrelid
           JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
           WHERE pg_namespace.nspname = 'pgflow'
             AND pg_class.relname = $1
             AND pg_constraint.conname = 'queue_name_is_valid'
           """,
           [table]
         ) do
      {:ok, %{num_rows: 1}} ->
        :ok

      {:ok, %{num_rows: 0}} ->
        {:error, "pgflow.#{table} is missing queue_name_is_valid check constraint"}

      {:error, error} ->
        {:error, "failed to inspect pgflow.#{table} queue constraint: #{inspect(error)}"}
    end
  end

  defp function_exists?(repo, signature) do
    case repo_query(repo, "SELECT to_regprocedure($1::text) IS NOT NULL", [signature]) do
      {:ok, %{rows: [[true]]}} -> true
      _ -> false
    end
  end

  defp repo_query(repo, sql, params \\ []) do
    SQL.query(repo, sql, params)
  end
end
