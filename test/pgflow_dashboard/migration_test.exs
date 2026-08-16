defmodule PgFlowDashboard.MigrationTest do
  @moduledoc """
  End-to-end tests for the dashboard schema migration (EctoEvolver-backed),
  focused on the v02 "skipped is a terminal state" changes.

  `test/test_helper.exs` does not apply `PgFlowDashboard.Migration` (only the
  core `PgFlow.Migration` + `PgFlow.HelpersMigration` are bootstrapped there),
  so this test applies/rolls back the dashboard schema itself. Like
  `PgFlow.MigrationTest`, it makes real (non-sandboxed) DDL + data changes
  against `pgflow.*` and the `pgflow_dashboard` schema, so it's tagged
  `:migration` and excluded from the default `mix test` run.

  Run with: `mix test --only migration test/pgflow_dashboard/migration_test.exs`
  """

  use ExUnit.Case, async: false

  @moduletag :migration

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo

  defmodule UpMigration do
    @moduledoc false
    use Ecto.Migration
    def change, do: PgFlowDashboard.Migration.up()
  end

  defmodule DownMigration do
    @moduledoc false
    use Ecto.Migration
    # Target version 1, not the default full uninstall (version 0): this test
    # is about v02 -> v01 reversibility (the brief's "down restores the v01
    # signatures"), not the separate v01_down.sql full-teardown path.
    def change, do: PgFlowDashboard.Migration.down(version: 1)
  end

  @up_version 1_100_000_000_001
  @down_version 1_100_000_000_002

  @flow_slug "dashboard_v02_smoke"

  # v01_up.sql's list_crons/count_crons/get_cron reference `cron.job` (they're
  # LANGUAGE sql functions, so Postgres validates the reference at CREATE
  # time). The atlas test image doesn't register pg_cron against our test
  # DB (see test_helper.exs), so stub the one table those functions touch -
  # same approach test_helper.exs uses for `realtime.*`.
  setup_all do
    Sandbox.mode(TestRepo, :auto)

    TestRepo.query!("CREATE SCHEMA IF NOT EXISTS cron")

    TestRepo.query!("""
    CREATE TABLE IF NOT EXISTS cron.job (
      jobid bigint,
      jobname text,
      schedule text,
      active boolean
    )
    """)

    Sandbox.mode(TestRepo, :manual)
    :ok
  end

  setup do
    Sandbox.mode(TestRepo, :auto)

    on_exit(fn ->
      cleanup_flow()
      TestRepo.query!("DROP SCHEMA IF EXISTS pgflow_dashboard CASCADE")

      TestRepo.query!(
        "DELETE FROM schema_migrations WHERE version IN ($1, $2)",
        [@up_version, @down_version]
      )

      Sandbox.mode(TestRepo, :manual)
    end)

    cleanup_flow()
    TestRepo.query!("DROP SCHEMA IF EXISTS pgflow_dashboard CASCADE")

    TestRepo.query!(
      "DELETE FROM schema_migrations WHERE version IN ($1, $2)",
      [@up_version, @down_version]
    )

    :ok
  end

  defp run_up!, do: Ecto.Migrator.up(TestRepo, @up_version, UpMigration, log: false)
  defp run_down!, do: Ecto.Migrator.up(TestRepo, @down_version, DownMigration, log: false)

  defp cleanup_flow do
    TestRepo.query!("DELETE FROM pgflow.step_tasks WHERE flow_slug = $1", [@flow_slug])
    TestRepo.query!("DELETE FROM pgflow.step_states WHERE flow_slug = $1", [@flow_slug])
    TestRepo.query!("DELETE FROM pgflow.runs WHERE flow_slug = $1", [@flow_slug])
    TestRepo.query!("DELETE FROM pgflow.steps WHERE flow_slug = $1", [@flow_slug])
    TestRepo.query!("DELETE FROM pgflow.flows WHERE flow_slug = $1", [@flow_slug])
  end

  # Seeds a completed run with one completed step and one started-then-skipped
  # step, entirely via direct row construction (bypassing the worker/state
  # machine) so timestamps are fully controlled.
  #
  # `step_a` completed normally. `step_b` was started an hour ago but its
  # condition became unmet after exhausting retries, so it was skipped only
  # 5 seconds after starting - completed_at stays NULL forever. Pre-fix, the
  # view's duration formula fell back to NOW() for a NULL completed_at,
  # producing a duration that grows without bound instead of the true ~5s.
  defp seed_run! do
    TestRepo.query!("INSERT INTO pgflow.flows (flow_slug) VALUES ($1)", [@flow_slug])

    TestRepo.query!(
      "INSERT INTO pgflow.steps (flow_slug, step_slug, step_index) VALUES ($1, 'step_a', 0)",
      [@flow_slug]
    )

    TestRepo.query!(
      "INSERT INTO pgflow.steps (flow_slug, step_slug, step_index) VALUES ($1, 'step_b', 1)",
      [@flow_slug]
    )

    run_id = Ecto.UUID.generate()
    run_id_bin = Ecto.UUID.dump!(run_id)

    started_at =
      DateTime.utc_now() |> DateTime.add(-3600, :second) |> DateTime.truncate(:microsecond)

    step_a_completed_at = DateTime.add(started_at, 2, :second)
    step_b_skipped_at = DateTime.add(started_at, 5, :second)
    run_completed_at = DateTime.add(started_at, 10, :second)

    TestRepo.query!(
      """
      INSERT INTO pgflow.runs
        (run_id, flow_slug, status, input, output, remaining_steps, started_at, completed_at)
      VALUES ($1, $2, 'completed', '{}'::jsonb, '{}'::jsonb, 0, $3, $4)
      """,
      [run_id_bin, @flow_slug, started_at, run_completed_at]
    )

    TestRepo.query!(
      """
      INSERT INTO pgflow.step_states
        (flow_slug, run_id, step_slug, status, remaining_tasks, remaining_deps,
         created_at, started_at, completed_at)
      VALUES ($1, $2, 'step_a', 'completed', 0, 0, $3, $3, $4)
      """,
      [@flow_slug, run_id_bin, started_at, step_a_completed_at]
    )

    TestRepo.query!(
      """
      INSERT INTO pgflow.step_states
        (flow_slug, run_id, step_slug, status, remaining_tasks, remaining_deps,
         created_at, started_at, skip_reason, skipped_at)
      VALUES ($1, $2, 'step_b', 'skipped', NULL, 0, $3, $3, 'handler_failed', $4)
      """,
      [@flow_slug, run_id_bin, started_at, step_b_skipped_at]
    )

    run_id
  end

  describe "up/0 (v02)" do
    setup do
      run_up!()
      run_id = seed_run!()
      %{run_id: run_id}
    end

    test "progress_percent counts skipped steps as resolved", %{run_id: run_id} do
      %{rows: [[progress_percent]]} =
        TestRepo.query!(
          "SELECT progress_percent FROM pgflow_dashboard.runs_with_progress WHERE run_id = $1",
          [Ecto.UUID.dump!(run_id)]
        )

      assert Decimal.equal?(progress_percent, Decimal.new("100.0"))
    end

    test "the run reports one completed and one skipped step", %{run_id: run_id} do
      %{rows: [[completed_steps, skipped_steps, total_steps]]} =
        TestRepo.query!(
          """
          SELECT completed_steps, skipped_steps, total_steps
          FROM pgflow_dashboard.runs_with_progress WHERE run_id = $1
          """,
          [Ecto.UUID.dump!(run_id)]
        )

      assert completed_steps == 1
      assert skipped_steps == 1
      assert total_steps == 2
    end

    test "a started-then-skipped step's duration is finite (bounded by skipped_at, not NOW())", %{
      run_id: run_id
    } do
      %{rows: [[duration_ms]]} =
        TestRepo.query!(
          """
          SELECT duration_ms FROM pgflow_dashboard.step_states_with_tasks
          WHERE run_id = $1 AND step_slug = 'step_b'
          """,
          [Ecto.UUID.dump!(run_id)]
        )

      duration_ms = Decimal.to_float(duration_ms)

      # Correct: skipped_at - started_at ~= 5_000ms.
      # Buggy (pre-fix): NOW() - started_at ~= 3_600_000ms (started_at is an
      # hour in the past), since the old formula fell back to NOW() whenever
      # completed_at was NULL.
      assert_in_delta duration_ms, 5_000, 2_000
    end

    test "skip_reason and skipped_at populate on the skipped step", %{run_id: run_id} do
      %{rows: [[skip_reason, skipped_at]]} =
        TestRepo.query!(
          """
          SELECT skip_reason, skipped_at FROM pgflow_dashboard.step_states_with_tasks
          WHERE run_id = $1 AND step_slug = 'step_b'
          """,
          [Ecto.UUID.dump!(run_id)]
        )

      assert skip_reason == "handler_failed"
      refute is_nil(skipped_at)
    end

    test "list_step_states() function surfaces skip_reason/skipped_at", %{run_id: run_id} do
      %{rows: rows, columns: columns} =
        TestRepo.query!(
          "SELECT * FROM pgflow_dashboard.list_step_states($1)",
          [Ecto.UUID.dump!(run_id)]
        )

      by_slug =
        Enum.map(rows, fn row -> columns |> Enum.zip(row) |> Map.new() end)
        |> Map.new(fn row -> {row["step_slug"], row} end)

      assert by_slug["step_b"]["status"] == "skipped"
      assert by_slug["step_b"]["skip_reason"] == "handler_failed"
      refute is_nil(by_slug["step_b"]["skipped_at"])
    end

    test "list_runs() and get_run() surface skipped_steps", %{run_id: run_id} do
      %{rows: [run_row], columns: columns} =
        TestRepo.query!(
          "SELECT * FROM pgflow_dashboard.get_run($1)",
          [Ecto.UUID.dump!(run_id)]
        )

      run = columns |> Enum.zip(run_row) |> Map.new()
      assert run["skipped_steps"] == 1
      assert Decimal.equal?(run["progress_percent"], Decimal.new("100.0"))
    end
  end

  describe "down/0 (v02 -> v01)" do
    test "restores the v01 view/function signatures" do
      run_up!()
      run_down!()

      # runs_with_progress no longer has skipped_steps.
      columns = view_columns("runs_with_progress")
      refute "skipped_steps" in columns
      assert "progress_percent" in columns

      # step_states_with_tasks no longer has skip_reason/skipped_at.
      step_columns = view_columns("step_states_with_tasks")
      refute "skip_reason" in step_columns
      refute "skipped_at" in step_columns

      # list_step_states() reverts to the v01 column set.
      list_step_states_sig = function_result_signature("list_step_states")
      refute list_step_states_sig =~ "skip_reason"
      refute list_step_states_sig =~ "skipped_at"

      # list_runs()/get_run() revert to the v01 column set.
      refute function_result_signature("list_runs") =~ "skipped_steps"
      refute function_result_signature("get_run") =~ "skipped_steps"

      # Version tracking comment reverts to 1.
      %{rows: [[comment]]} =
        TestRepo.query!("""
        SELECT obj_description(('pgflow_dashboard.runs_with_progress')::regclass, 'pg_class')
        """)

      assert comment =~ ~r/version=1\b/
    end

    test "count_runs() and get_run_history_grid() survive the round trip untouched" do
      run_up!()
      seed_run!()
      run_down!()

      assert {:ok, %{rows: [[count]]}} =
               TestRepo.query(
                 "SELECT * FROM pgflow_dashboard.count_runs($1, $2, $3, $4)",
                 [~U[2000-01-01 00:00:00.000000Z], nil, nil, nil]
               )

      assert is_integer(count)

      assert {:ok, _} =
               TestRepo.query(
                 "SELECT * FROM pgflow_dashboard.get_run_history_grid($1, $2)",
                 [@flow_slug, 10]
               )
    end
  end

  defp view_columns(view_name) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pgflow_dashboard' AND table_name = $1
        """,
        [view_name]
      )

    Enum.map(rows, fn [name] -> name end)
  end

  # Returns the RETURNS TABLE(...) signature text for a pgflow_dashboard
  # function, e.g. "TABLE(run_id uuid, ..., skipped_steps bigint, ...)", so
  # callers can assert on column presence/absence without having to parse
  # multi-word Postgres type names (e.g. "timestamp with time zone").
  defp function_result_signature(function_name) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT pg_get_function_result(p.oid)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgflow_dashboard' AND p.proname = $1
        """,
        [function_name]
      )

    case rows do
      [[definition]] -> definition
      _ -> ""
    end
  end
end
