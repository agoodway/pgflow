defmodule PgFlow.Upstream.Spec.V03 do
  @moduledoc """
  Import rules for core V03, taken from upstream pgflow 0.17.2.

  The pinned sources are the private-queue migration, the force-skip fix, and
  the four `pgflow` DDL statements at the top of the telemetry migration. The
  `pgflow_telemetry` schema is not imported.
  """

  alias PgFlow.Sql.Splitter
  alias PgFlow.Upgrade.SlugAudit

  alias PgFlow.Upstream.Bundle

  import PgFlow.Upstream.Spec.Matchers,
    only: [localize_lock_timeout: 1, set_lock_timeout?: 1, start_tasks_definition?: 1]

  @pinned_sha "baa9e43f5f9122133deacbc3e8ffc80e284b4f50"
  @files [
    "20260915074120_pgflow_private_step_queues.sql",
    "20260919152659_pgflow_fix_force_skip_multi_queue.sql",
    "20260920093533_pgflow_telemetry.sql"
  ]
  @source_hashes %{
    "20260915074120_pgflow_private_step_queues.sql" =>
      "571fc92496fbfcf2906717e234c54c970207b4f29981fe965a1e0a6838d55237",
    "20260919152659_pgflow_fix_force_skip_multi_queue.sql" =>
      "9c37b2cd20b39e4d13ebc40b4910485bab4f9f08ba1f01b9f3159882c81dadba",
    "20260920093533_pgflow_telemetry.sql" =>
      "86b47ecc6aba8014c46ebd1107e4a6208f76896b76fe7cb82d5770fb0fa1e9cd"
  }
  @telemetry_file "20260920093533_pgflow_telemetry.sql"
  @three_arg_drop String.trim("""
                  DROP FUNCTION "pgflow"."start_tasks" (text, bigint[], uuid)
                  """)
  @v02_split "\n\n--SPLIT--\n\n"

  # Whitespace-collapsed, quote-stripped pairs. The upstream side is what V03
  # omits. The V02 side must still be present in the published v02_up.sql.
  @pairs [
    # idx_flows
    {"-- Create index idx_flows_normalized_slug to table: flows CREATE UNIQUE INDEX idx_flows_no" <>
       "rmalized_slug ON pgflow.flows ((lower(flow_slug)))",
     "-- Create index idx_flows_normalized_slug to table: flows -- Created after the backfill (a" <>
       "pproved staged order): conflicting normalized -- flow slugs fail this statement and roll t" <>
       "he whole migration back atomically. CREATE UNIQUE INDEX idx_flows_normalized_slug ON pgflo" <>
       "w.flows ((lower(flow_slug)))"},
    # is_valid_queue_name
    {"-- Create is_valid_queue_name function CREATE FUNCTION pgflow.is_valid_queue_name (queue_n" <>
       "ame text) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path = '' AS $$ -- Mirrors pgm" <>
       "q.validate_queue_name() (47-character limit) and additionally -- requires the canonical lo" <>
       "wercase spelling pgflow stores (#650). select queue_name is not null and queue_name <> '' " <>
       "and length(queue_name) <= 47 and queue_name = lower(queue_name) $$",
     "-- Create is_valid_queue_name function CREATE FUNCTION pgflow.is_valid_queue_name (queue_n" <>
       "ame text) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path = '' AS $$ -- Mirrors pgm" <>
       "q.validate_queue_name() (47-character limit) and additionally -- requires the canonical lo" <>
       "wercase spelling pgflow stores (#650). select queue_name is not null and queue_name <> '' " <>
       "and length(queue_name) <= 47 and queue_name = lower(queue_name) $$"},
    # drop_idx_message
    {"-- Drop index idx_step_tasks_message_id from table: step_tasks DROP INDEX pgflow.idx_step_" <>
       "tasks_message_id",
     "-- Drop index idx_step_tasks_message_id from table: step_tasks -- Replaced by the queue-sc" <>
       "oped unique index below (#650) DROP INDEX pgflow.idx_step_tasks_message_id"},
    # add_col_tasks
    {"-- Modify step_tasks table -- Atlas data-transition limitation: a final-state schema canno" <>
       "t express -- the backfill, so stage it: add the column permissively (the NOT NULL and -- t" <>
       "he CHECK both reject NULL and must not run before existing rows are -- backfilled), backfi" <>
       "ll every existing task with the 0.16 canonical queue -- (lower(flow_slug)), then enforce N" <>
       "OT NULL and the CHECK. All inside this -- single-transaction migration. ALTER TABLE pgflow" <>
       ".step_tasks ADD COLUMN queue_name text",
     "-- Modify step_tasks table (staged: nullable column, backfill, then constraints) ALTER TAB" <>
       "LE pgflow.step_tasks ADD COLUMN queue_name text NULL"},
    # update_tasks
    {"UPDATE pgflow.step_tasks SET queue_name = lower(flow_slug) WHERE queue_name IS NULL",
     "UPDATE pgflow.step_tasks SET queue_name = lower(flow_slug) WHERE queue_name IS NULL"},
    # alter_tasks
    {"ALTER TABLE pgflow.step_tasks ADD CONSTRAINT queue_name_is_valid CHECK (pgflow.is_valid_qu" <>
       "eue_name(queue_name)), ALTER COLUMN queue_name SET NOT NULL",
     "ALTER TABLE pgflow.step_tasks ALTER COLUMN queue_name SET NOT NULL, ADD CONSTRAINT queue_n" <>
       "ame_is_valid CHECK (pgflow.is_valid_queue_name(queue_name))"},
    # idx_queue_message
    {"-- Create index idx_step_tasks_queue_message to table: step_tasks CREATE UNIQUE INDEX idx_" <>
       "step_tasks_queue_message ON pgflow.step_tasks (queue_name, message_id) WHERE (message_id I" <>
       "S NOT NULL)",
     "-- Create index idx_step_tasks_queue_message to table: step_tasks -- Created after the bac" <>
       "kfill (approved staged order): duplicate -- (queue_name, message_id) pairs fail this state" <>
       "ment and roll the whole -- migration back atomically. CREATE UNIQUE INDEX idx_step_tasks_q" <>
       "ueue_message ON pgflow.step_tasks (queue_name, message_id) WHERE (message_id IS NOT NULL)"},
    # add_col_steps
    {"-- Modify steps table -- Same staged data transition as step_tasks: permissive column, bac" <>
       "kfill -- each persisted route with lower(flow_slug), then enforce NOT NULL and -- the CHEC" <>
       "K (which rejects NULL and must follow the backfill). ALTER TABLE pgflow.steps ADD COLUMN q" <>
       "ueue_name text",
     "-- Modify steps table (staged: nullable column, backfill, then constraints) ALTER TABLE pg" <>
       "flow.steps ADD COLUMN queue_name text NULL"},
    # update_steps
    {"UPDATE pgflow.steps SET queue_name = lower(flow_slug) WHERE queue_name IS NULL",
     "-- ========================================== -- DATA BACKFILL: queue identity snapshots (" <>
       "#650) -- Every step and task routes to its flow's default queue: lower(flow_slug). -- Incl" <>
       "udes tasks whose message_id is NULL. Duplicate identities, queue names -- beyond PGMQ's li" <>
       "mit, or conflicting normalized flow slugs raise in the -- constraint and index statements " <>
       "below and leave the database unchanged; -- they are not repaired automatically. -- =======" <>
       "=================================== UPDATE pgflow.steps SET queue_name = lower(flow_slug) " <>
       "WHERE queue_name IS NULL"},
    # alter_steps
    {"ALTER TABLE pgflow.steps ADD CONSTRAINT queue_name_is_valid CHECK (pgflow.is_valid_queue_n" <>
       "ame(queue_name)), ALTER COLUMN queue_name SET NOT NULL",
     "ALTER TABLE pgflow.steps ALTER COLUMN queue_name SET NOT NULL, ADD CONSTRAINT queue_name_i" <>
       "s_valid CHECK (pgflow.is_valid_queue_name(queue_name))"},
    # listed
    {"-- Create _listed_queue_name function CREATE FUNCTION pgflow._listed_queue_name (p_queue_n" <>
       "ame text) RETURNS text LANGUAGE plpgsql STABLE SET search_path = '' AS $$ declare v_matche" <>
       "s text[]; begin -- Resolve every listed spelling of the normalized name before preferring " <>
       "-- any single match: an ambiguous pair is rejected even when one spelling -- is the exact " <>
       "requested name (#650). select array_agg(listed.queue_name order by listed.queue_name) into" <>
       " v_matches from pgmq.list_queues() as listed where lower(listed.queue_name) = lower(p_queu" <>
       "e_name); if v_matches is null then -- Not listed: pass through; PGMQ reports its own error" <>
       " (or no-ops) return p_queue_name; elsif cardinality(v_matches) > 1 then raise exception 'q" <>
       "ueue name % is ambiguous: it matches listed queues %', p_queue_name, v_matches using errco" <>
       "de = 'ambiguous_alias'; else return v_matches[1]; end if; end; $$",
     "-- Create _listed_queue_name function CREATE FUNCTION pgflow._listed_queue_name (p_queue_n" <>
       "ame text) RETURNS text LANGUAGE plpgsql STABLE SET search_path = '' AS $$ declare v_matche" <>
       "s text[]; begin -- Resolve every listed spelling of the normalized name before preferring " <>
       "-- any single match: an ambiguous pair is rejected even when one spelling -- is the exact " <>
       "requested name (#650). select array_agg(listed.queue_name order by listed.queue_name) into" <>
       " v_matches from pgmq.list_queues() as listed where lower(listed.queue_name) = lower(p_queu" <>
       "e_name); if v_matches is null then -- Not listed: pass through; PGMQ reports its own error" <>
       " (or no-ops) return p_queue_name; elsif cardinality(v_matches) > 1 then raise exception 'q" <>
       "ueue name % is ambiguous: it matches listed queues %', p_queue_name, v_matches using errco" <>
       "de = 'ambiguous_alias'; else return v_matches[1]; end if; end; $$"}
  ]

  @doc "Upstream commit whose bytes produce core V03."
  @spec pinned_sha() :: String.t()
  def pinned_sha, do: @pinned_sha

  @doc "Upstream migration filenames, in import order."
  @spec files() :: [String.t()]
  def files, do: @files

  @doc "SHA-256 of each pinned upstream file."
  @spec source_hashes() :: %{String.t() => String.t()}
  def source_hashes, do: @source_hashes

  @doc """
  Per-file import mode.

  The telemetry migration keeps only the four `pgflow` DDL statements. The
  kept count is seeded onto `:telemetry_pgflow_ddl_allowlist` before the
  statement rules run, so `replace_desc_runs_index` can still see the index.
  """
  @spec file_modes() :: map()
  def file_modes do
    %{
      @telemetry_file => %{
        mode: :pgflow_ddl_allowlist,
        rule: :telemetry_pgflow_ddl_allowlist,
        keep: &telemetry_keep?/1
      }
    }
  end

  @doc """
  Fails unless every omission pair is present on both sides.

  `opts[:v02_sql]` replaces the committed V02 script. A missing V02 side
  returns `omit_applied_in_v02: V02 counterpart missing for`.
  """
  @spec verify_omissions([String.t()], keyword()) :: :ok | {:error, String.t()}
  def verify_omissions(statements, opts) when is_list(statements) and is_list(opts) do
    upstream = MapSet.new(statements, &normalize/1)
    v02 = MapSet.new(v02_statements(opts), &normalize/1)

    Enum.find_value(@pairs, :ok, fn pair ->
      missing_counterpart(pair, upstream, v02)
    end)
  end

  @doc """
  Portability rules for the 0.17.2 import.

  The first matching rule wins. `:reject` and the seeded allowlist are
  expected to match nothing in the statements that reach the rule engine.
  `slug_preflight` is appended after the localized lock timeout.
  """
  @spec rules() :: [map()]
  def rules do
    [
      %{
        id: :reject_net_http_post,
        match: &net_http_post?/1,
        action: :reject,
        expected: 0,
        reason:
          "Guard carried over from V02. None of the imported 0.17.2 statements " <>
            "call net.http_post, so a hit fails the build."
      },
      %{
        id: :set_local_lock_timeout,
        match: &set_lock_timeout?/1,
        action: {:replace, &localize_lock_timeout/1},
        expected: 1,
        reason: "Existing V02 rule. Scopes the lock timeout to the Ecto transaction."
      },
      %{
        id: :slug_preflight,
        match: &never?/1,
        action: {:append_after, :set_local_lock_timeout, SlugAudit.preflight_sql("pgflow")},
        expected: 1,
        reason:
          "A DO block that runs the same checks as mix pgflow.audit_slugs and " <>
            "raises one check_violation listing every offending flow or step."
      },
      %{
        id: :omit_applied_in_v02,
        match: &omit_applied?/1,
        action: :omit,
        expected: 11,
        reason:
          "V02 already applied these statements from the deleted queue-identity migration. " <>
            "The pair table records the spellings that differ."
      },
      %{
        id: :drop_if_exists,
        match: &three_arg_start_tasks_drop?/1,
        action: {:replace, &add_if_exists/1},
        expected: 1,
        reason: "Already dropped by helpers V05 on upgraded databases."
      },
      %{
        id: :replace_desc_runs_index,
        match: &runs_started_at_index?/1,
        action: {:prepend, ~s(DROP INDEX IF EXISTS "pgflow"."idx_runs_started_at")},
        expected: 1,
        reason:
          "Emits DROP INDEX IF EXISTS pgflow.idx_runs_started_at so upstream's ascending index wins."
      },
      %{
        id: :step_task_record_dispatch,
        match: &start_tasks_definition?/1,
        action: {:replace, &Bundle.shape_dispatch_parts/1},
        expected: 1,
        reason:
          "Reuses the V02 dispatch: a DO block inspects pgflow.step_task_record and executes the 7- or 8-column body."
      },
      %{
        id: :telemetry_pgflow_ddl_allowlist,
        match: &never?/1,
        action: :omit,
        expected: 4,
        reason:
          "Keep the four pgflow DDL statements from the telemetry migration and exclude the pgflow_telemetry schema."
      }
    ]
  end

  @doc "Manifest fields that describe the V03 import but are not derived from the SQL."
  @spec manifest_notes() :: map()
  def manifest_notes do
    %{
      "removals" => [
        %{
          "object" => "pgflow_telemetry",
          "reason" =>
            "Supabase telemetry schema is out of scope; only the four pgflow DDL statements are imported"
        }
      ],
      "upstream_path" => "pkgs/core/supabase/migrations",
      "upstream_release" => "0.17.2",
      "upstream_repo" => "pgflow-dev/pgflow"
    }
  end

  defp missing_counterpart({upstream, v02}, upstream_set, v02_set) do
    cond do
      not MapSet.member?(upstream_set, upstream) ->
        {:error, "omit_applied_in_v02: upstream statement missing for #{pair_label(upstream)}"}

      not MapSet.member?(v02_set, v02) ->
        {:error, "omit_applied_in_v02: V02 counterpart missing for #{pair_label(upstream)}"}

      true ->
        nil
    end
  end

  defp pair_label(sql), do: String.slice(sql, 0, 80)

  defp v02_statements(opts) do
    sql =
      case Keyword.fetch(opts, :v02_sql) do
        {:ok, sql} when is_binary(sql) -> sql
        :error -> File.read!(v02_sql_path())
      end

    String.split(sql, @v02_split)
  end

  defp v02_sql_path do
    :pgflow
    |> :code.priv_dir()
    |> Path.join("pgflow_core/sql/versions/v02/v02_up.sql")
  end

  defp omit_applied?(statement) do
    normalized = normalize(statement)
    Enum.any?(@pairs, fn {upstream, _v02} -> upstream == normalized end)
  end

  defp normalize(sql) do
    sql
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> String.replace(~r/;$/, "")
    |> String.replace("\"", "")
  end

  defp telemetry_keep?(statement) do
    statement
    |> Splitter.strip_leading_comments()
    |> then(&Regex.match?(telemetry_pattern(), &1))
  end

  defp telemetry_pattern do
    index = ~S[CREATE INDEX "idx_(runs|workers)_(started|stopped)_at" ON "pgflow"\.]
    column = ~S[ALTER TABLE "pgflow"\."workers" ADD COLUMN "pgflow_version"]
    Regex.compile!("\\A(#{index}|#{column})")
  end

  defp net_http_post?(statement), do: statement =~ "net.http_post"

  defp never?(_statement), do: false

  defp three_arg_start_tasks_drop?(statement), do: String.contains?(statement, @three_arg_drop)

  defp add_if_exists(statement) do
    {:ok,
     String.replace(
       statement,
       ~s(DROP FUNCTION "pgflow"."start_tasks"),
       ~s(DROP FUNCTION IF EXISTS "pgflow"."start_tasks"),
       global: false
     )}
  end

  defp runs_started_at_index?(statement) do
    String.contains?(statement, ~s(CREATE INDEX "idx_runs_started_at"))
  end
end
