defmodule PgFlow.Upstream.Bundle do
  @moduledoc """
  Builds a deterministic core SQL delta from a versioned upstream spec.

  Published versions stay frozen. V02's import rules live in
  `PgFlow.Upstream.Spec.V02` and are not rebuilt. The next import is core V03.
  """

  alias PgFlow.Sql.Splitter

  @published ~w(01 02)
  @specs %{"03" => PgFlow.Upstream.Spec.V03}
  @seven_names ~w(flow_slug run_id step_slug input msg_id task_index flow_input)
  @seven_types ["text", "uuid", "text", "jsonb", "bigint", "integer", "jsonb"]
  @eight_names @seven_names ++ ["attempts_count"]
  @eight_types @seven_types ++ ["integer"]

  @doc "Core versions whose generated SQL must not be rewritten."
  @spec published_versions() :: [String.t()]
  def published_versions, do: @published

  @doc """
  Builds SQL and a provenance manifest for one core delta version.

  Published versions and unknown versions return `{:error, message}` without
  reading the checkout.
  """
  @spec build(Path.t(), String.t(), String.t()) ::
          {:ok, %{sql: String.t(), manifest: map()}} | {:error, String.t()}
  def build(checkout, sha, version)
      when is_binary(checkout) and is_binary(sha) and is_binary(version) do
    with :ok <- validate_version(version),
         spec = Map.fetch!(@specs, version),
         :ok <- verify_sha(sha, spec.pinned_sha()),
         :ok <- verify_checkout_head(checkout, sha),
         {:ok, sources} <- read_verified_sources(checkout, spec),
         {:ok, statements, counts} <- transform_sources(sources, spec.rules()) do
      sql = Splitter.join(statements) <> "\n"
      {:ok, %{sql: sql, manifest: manifest(spec, sources, statements, counts, sql)}}
    end
  end

  @doc """
  Accepts a version that has a loaded spec and is not published.

  `published` defaults to the frozen core versions. The mix task passes its
  own copy so the two lists stay wired together.
  """
  @spec validate_version(String.t(), [String.t()]) :: :ok | {:error, String.t()}
  def validate_version(version, published \\ @published)
      when is_binary(version) and is_list(published) do
    cond do
      version in published ->
        {:error, "refusing to rewrite published core version V#{version}"}

      not match?({:ok, _spec}, Map.fetch(@specs, version)) ->
        unsupported_version(version)

      not Code.ensure_loaded?(Map.fetch!(@specs, version)) ->
        unsupported_version(version)

      true ->
        :ok
    end
  end

  @doc """
  Wraps a `start_tasks` definition so it installs the seven-column body or the
  eight-column `attempts_count` body, depending on `pgflow.step_task_record`.
  """
  @spec shape_dispatch(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def shape_dispatch(seven_column_sql) do
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

  defp unsupported_version(version) do
    {:error, "unsupported core delta version #{inspect(version)}; expected 03"}
  end

  defp verify_sha(sha, sha), do: :ok

  defp verify_sha(sha, pinned) do
    {:error, "refusing unpinned upstream SHA #{inspect(sha)}; expected #{pinned}"}
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

  defp read_verified_sources(checkout, spec) do
    path_root = spec.manifest_notes()["upstream_path"]

    Enum.reduce_while(spec.files(), {:ok, []}, fn file, {:ok, sources} ->
      path = Path.join([checkout, path_root, file])

      case File.read(path) do
        {:ok, source} ->
          verify_source(spec, file, source, sources)

        {:error, reason} ->
          {:halt, {:error, "could not read #{path}: #{:file.format_error(reason)}"}}
      end
    end)
  end

  defp verify_source(spec, file, source, sources) do
    actual_hash = sha256(source)
    expected_hash = Map.fetch!(spec.source_hashes(), file)

    if actual_hash == expected_hash do
      {:cont, {:ok, sources ++ [{file, source}]}}
    else
      {:halt,
       {:error,
        "source #{file} does not match pinned #{spec.pinned_sha()} bytes: " <>
          "expected SHA-256 #{expected_hash}, got #{actual_hash}"}}
    end
  end

  defp transform_sources(sources, rules) do
    statements = Enum.flat_map(sources, fn {_file, source} -> Splitter.split(source) end)
    apply_rules(statements, rules)
  end

  defp apply_rules(statements, rules) do
    counts = Map.new(rules, &{&1.id, 0})

    statements
    |> Enum.reduce_while({:ok, [], counts}, fn statement, {:ok, acc, counts} ->
      reduce_statement(statement, rules, acc, counts)
    end)
    |> validate_rule_counts(rules)
  end

  defp reduce_statement(statement, rules, acc, counts) do
    case Enum.find(rules, & &1.match.(statement)) do
      nil ->
        {:cont, {:ok, acc ++ [statement], counts}}

      rule ->
        counts = Map.update!(counts, rule.id, &(&1 + 1))

        case apply_action(rule, statement) do
          :omit ->
            {:cont, {:ok, acc, counts}}

          {:ok, replacement} when is_binary(replacement) ->
            {:cont, {:ok, acc ++ [replacement], counts}}

          {:ok, replacements} when is_list(replacements) ->
            {:cont, {:ok, acc ++ replacements, counts}}

          {:error, _message} = error ->
            {:halt, error}
        end
    end
  end

  defp validate_rule_counts({:error, _} = error, _rules), do: error

  defp validate_rule_counts({:ok, statements, counts}, rules) do
    case count_error(rules, counts) do
      nil -> {:ok, statements, counts}
      message -> {:error, message}
    end
  end

  defp count_error(rules, counts) do
    Enum.find_value(rules, fn rule ->
      matched = Map.fetch!(counts, rule.id)

      if matched == rule.expected do
        nil
      else
        "rule #{rule.id} matched #{matched} statements; expected #{rule.expected}"
      end
    end)
  end

  defp apply_action(%{action: :omit}, _statement), do: :omit

  defp apply_action(%{action: :reject, reason: reason}, _statement), do: {:error, reason}

  defp apply_action(%{action: {:replace, fun}}, statement), do: fun.(statement)

  defp apply_action(%{action: {:prepend, sql}}, statement) when is_binary(sql) do
    {:ok, [sql, statement]}
  end

  defp manifest(spec, sources, statements, counts, sql) do
    transformations =
      Enum.map(spec.rules(), fn rule ->
        %{
          "rule" => Atom.to_string(rule.id),
          "matched" => Map.fetch!(counts, rule.id),
          "reason" => rule.reason
        }
      end)

    spec.manifest_notes()
    |> Map.merge(%{
      "files" => spec.files(),
      "output_sha256" => sha256(sql),
      "source_sha256" => Map.new(sources, fn {file, source} -> {file, sha256(source)} end),
      "statement_count" => length(statements),
      "transformations" => transformations,
      "upstream_sha" => spec.pinned_sha()
    })
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
