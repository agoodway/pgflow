defmodule PgFlow.Upstream.Bundle do
  @moduledoc """
  Builds the deterministic V02 core delta from the pinned upstream checkout.

  The import is deliberately narrow: only the four reviewed migrations are
  accepted, and every portability change is represented in the manifest.
  """

  alias PgFlow.Sql.Splitter

  @pinned_sha "94490709f79ebf366141dd925b047f0c1013e759"
  @upstream_path "pkgs/core/supabase/migrations"
  @files [
    "20260607175525_pgflow_worker_start_mode.sql",
    "20260904095427_pgflow_task_lifecycle_hardening.sql",
    "20260907082520_pgflow_remove_legacy_flow_compilation.sql",
    "20260913093141_pgflow_persist_queue_identity.sql"
  ]
  @source_hashes %{
    "20260607175525_pgflow_worker_start_mode.sql" =>
      "29c66ed86b85e2ae5693fc49ec410c901bfeebf320cda2a02720dccda233640a",
    "20260904095427_pgflow_task_lifecycle_hardening.sql" =>
      "41fb8e26642685f2269aa0bcc9bd4e9a512292bd7f2467f59d3577e1687f2d68",
    "20260907082520_pgflow_remove_legacy_flow_compilation.sql" =>
      "bc10af88b655a84f271a5aa5aea79f8ac74680e423293f8be1fac5656b4983b0",
    "20260913093141_pgflow_persist_queue_identity.sql" =>
      "156de3b8a73812cdba59ee19af1ec8524834f74209c9cb914a2d575c33fe505a"
  }
  @seven_names ~w(flow_slug run_id step_slug input msg_id task_index flow_input)
  @seven_types ["text", "uuid", "text", "jsonb", "bigint", "integer", "jsonb"]
  @eight_names @seven_names ++ ["attempts_count"]
  @eight_types @seven_types ++ ["integer"]

  @doc "Builds V02 SQL and its deterministic provenance manifest."
  @spec build(Path.t(), String.t()) ::
          {:ok, %{sql: String.t(), manifest: map()}} | {:error, String.t()}
  def build(checkout, sha) when is_binary(checkout) and is_binary(sha) do
    with :ok <- verify_sha(sha),
         :ok <- verify_checkout_head(checkout, sha),
         {:ok, sources} <- read_verified_sources(checkout),
         {:ok, statements} <- transform_sources(sources) do
      sql = Splitter.join(statements) <> "\n"

      manifest = %{
        "files" => @files,
        "output_sha256" => sha256(sql),
        "removals" => [
          %{
            "object" => "pgflow.ensure_workers()",
            "reason" => "requires Supabase-only net.http_post"
          }
        ],
        "source_sha256" => Map.new(sources, fn {file, source} -> {file, sha256(source)} end),
        "statement_count" => length(statements),
        "transformations" => [
          %{
            "change" => "SET lock_timeout = '10s' -> SET LOCAL lock_timeout = '10s'",
            "reason" => "transaction-local setting does not leak into pooled connections"
          },
          %{
            "change" =>
              "start_tasks installations dispatch on seven- or eight-column step_task_record",
            "reason" => "preserve the Elixir attempts_count extension"
          }
        ],
        "upstream_path" => @upstream_path,
        "upstream_repo" => "pgflow-dev/pgflow",
        "upstream_sha" => @pinned_sha
      }

      {:ok, %{sql: sql, manifest: manifest}}
    end
  end

  defp verify_sha(@pinned_sha), do: :ok

  defp verify_sha(sha) do
    {:error, "refusing unpinned upstream SHA #{inspect(sha)}; expected #{@pinned_sha}"}
  end

  defp verify_checkout_head(checkout, sha) do
    case System.cmd("git", ["-C", checkout, "rev-parse", "HEAD"], stderr_to_stdout: true) do
      {head, 0} ->
        head = String.trim(head)

        if head == sha do
          :ok
        else
          {:error,
           "checkout HEAD #{head} does not match requested SHA #{sha}; " <>
             "refusing to read sources from a mismatched checkout"}
        end

      {output, status} ->
        {:error,
         "could not read checkout HEAD via git rev-parse (exit #{status}): #{String.trim(output)}"}
    end
  end

  defp read_verified_sources(checkout) do
    Enum.reduce_while(@files, {:ok, []}, fn file, {:ok, sources} ->
      path = Path.join([checkout, @upstream_path, file])

      case File.read(path) do
        {:ok, source} ->
          verify_source(file, source, sources)

        {:error, reason} ->
          {:halt, {:error, "could not read #{path}: #{:file.format_error(reason)}"}}
      end
    end)
  end

  defp verify_source(file, source, sources) do
    actual_hash = sha256(source)
    expected_hash = Map.fetch!(@source_hashes, file)

    if actual_hash == expected_hash do
      {:cont, {:ok, sources ++ [{file, source}]}}
    else
      {:halt,
       {:error,
        "source #{file} does not match pinned #{@pinned_sha} bytes: " <>
          "expected SHA-256 #{expected_hash}, got #{actual_hash}"}}
    end
  end

  defp transform_sources(sources) do
    sources
    |> Enum.flat_map(fn {_file, source} -> Splitter.split(source) end)
    |> Enum.reduce_while({:ok, []}, fn statement, {:ok, transformed} ->
      case transform_statement(statement) do
        :omit -> {:cont, {:ok, transformed}}
        {:ok, replacement} -> {:cont, {:ok, transformed ++ [replacement]}}
        {:error, _message} = error -> {:halt, error}
      end
    end)
  end

  defp transform_statement(statement) do
    cond do
      ensure_workers_definition?(statement) ->
        if statement =~ "net.http_post" do
          :omit
        else
          {:error, "refusing to omit pgflow.ensure_workers(): expected net.http_post dependency"}
        end

      statement =~ "net.http_post" ->
        {:error, "unrecognized net.http_post infrastructure dependency"}

      Regex.match?(~r/^SET lock_timeout = '10s'$/m, statement) ->
        {:ok, Regex.replace(~r/^SET lock_timeout/m, statement, "SET LOCAL lock_timeout")}

      start_tasks_definition?(statement) ->
        shape_dispatch(statement)

      true ->
        {:ok, statement}
    end
  end

  defp ensure_workers_definition?(statement) do
    Regex.match?(
      ~r/CREATE OR REPLACE FUNCTION "pgflow"\."ensure_workers" \(\) RETURNS TABLE/s,
      statement
    )
  end

  defp start_tasks_definition?(statement) do
    Regex.match?(~r/CREATE (?:OR REPLACE )?FUNCTION "pgflow"\."start_tasks" /s, statement)
  end

  defp shape_dispatch(seven_column_sql) do
    with {:ok, eight_column_sql} <- add_attempts_count(seven_column_sql) do
      {:ok,
       """
       DO $pgflow_shape_dispatch$
       DECLARE
         attribute_names text[];
         attribute_types text[];
       BEGIN
         SELECT
           array_agg(attribute.attname ORDER BY attribute.attnum),
           array_agg(format_type(attribute.atttypid, attribute.atttypmod) ORDER BY attribute.attnum)
         INTO attribute_names, attribute_types
         FROM pg_type AS composite
         JOIN pg_class AS relation ON relation.oid = composite.typrelid
         JOIN pg_attribute AS attribute ON attribute.attrelid = relation.oid
         WHERE composite.oid = 'pgflow.step_task_record'::regtype
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped;

         IF attribute_names = #{sql_array(@seven_names)}
            AND attribute_types = #{sql_array(@seven_types)} THEN
           EXECUTE $pgflow_seven$
       #{seven_column_sql}
       $pgflow_seven$;
         ELSIF attribute_names = #{sql_array(@eight_names)}
               AND attribute_types = #{sql_array(@eight_types)} THEN
           EXECUTE $pgflow_eight$
       #{eight_column_sql}
       $pgflow_eight$;
         ELSE
           RAISE EXCEPTION 'unsupported pgflow.step_task_record layout: names=%, types=%',
             attribute_names, attribute_types
             USING HINT = 'Expected the seven-column upstream layout or its eight-column Elixir attempts_count extension.';
         END IF;
       END
       $pgflow_shape_dispatch$
       """
       |> String.trim()}
    end
  end

  defp add_attempts_count(sql) do
    returning_before = """
        returning
          step_tasks.flow_slug,
          step_tasks.run_id,
          step_tasks.step_slug,
          step_tasks.task_index,
          step_tasks.message_id
    """

    returning_after =
      returning_before
      |> String.trim_trailing()
      |> Kernel.<>(",\n      step_tasks.attempts_count\n")

    projection_before = """
          ELSE NULL
        END as flow_input
      from tasks st
    """

    projection_after = """
          ELSE NULL
        END as flow_input,
        st.attempts_count
      from tasks st
    """

    case replace_once(sql, returning_before, returning_after, "guarded UPDATE RETURNING") do
      {:ok, sql} -> replace_once(sql, projection_before, projection_after, "final projection")
      {:error, _message} = error -> error
    end
  end

  defp replace_once(subject, old, new, description) do
    case :binary.matches(subject, old) do
      [_match] -> {:ok, String.replace(subject, old, new)}
      matches -> {:error, "expected one #{description} in start_tasks, found #{length(matches)}"}
    end
  end

  defp sql_array(values) do
    values
    |> Enum.map_join(", ", &"'#{&1}'")
    |> then(&"ARRAY[#{&1}]::text[]")
  end

  defp sha256(content), do: :crypto.hash(:sha256, content) |> Base.encode16(case: :lower)
end
