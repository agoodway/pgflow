defmodule PgFlow.Upstream.InteroperabilityTest do
  use ExUnit.Case, async: false

  alias PgFlow.Test.{UpstreamFixture, UpstreamHarness, UpstreamInterop}

  @moduletag :integration
  @pinned_sha UpstreamHarness.pinned_sha()

  describe "harness guards" do
    @tag :harness

    test "rejects wrong upstream SHA" do
      assert {:error, message} =
               UpstreamHarness.run(
                 checkout: "/tmp/unused",
                 sha: "deadbeef",
                 database_url: "postgres://localhost/pgflow_compat_test",
                 suite: :catalog
               )

      assert message =~ "refusing unpinned upstream SHA"
    end

    @tag :upstream_checkout
    test "rejects non-fixture database targets" do
      checkout = System.fetch_env!("PGFLOW_UPSTREAM_CHECKOUT")

      assert {:error, message} =
               UpstreamHarness.run(
                 checkout: checkout,
                 sha: @pinned_sha,
                 database_url: UpstreamFixture.application_database_url(),
                 suite: :catalog
               )

      assert message =~ "refusing non-fixture database"
    end
  end

  @pgtap_parity_files [
    "is_valid_slug.test.sql",
    "ensure_flow_compiled/auto_recompiles_when_local.test.sql",
    "ensure_flow_compiled/compiles_missing_flow.test.sql",
    "ensure_flow_compiled/concurrent_compilation_race.test.sql",
    "ensure_flow_compiled/fails_mismatch_when_not_local.test.sql",
    "ensure_flow_compiled/signature.test.sql",
    "ensure_flow_compiled/verifies_matching_shape.test.sql",
    "queue_mode/delete_and_prune_routes.test.sql",
    "queue_mode/naming_restrictions.test.sql",
    "queue_mode/owned_route_requires_listed_queue.test.sql",
    "queue_mode/queue_name_resolution.test.sql",
    "queue_mode/route_map_verification.test.sql",
    "queue_mode/start_tasks_step_selector.test.sql",
    "queue_mode/step_mode_provisioning.test.sql",
    "queue_mode/step_worker_batch_safety.test.sql",
    "queue_mode/two_step_slice.test.sql",
    "queue_mode/worker_route_coverage.test.sql",
    "_cascade_force_skip_steps/archives_task_messages_for_skipped_steps.test.sql",
    "_cascade_force_skip_steps/archives_task_messages_from_all_private_queues.test.sql",
    "_cascade_force_skip_steps/broadcast_order.test.sql",
    "_cascade_force_skip_steps/cascade_through_multiple_levels.test.sql",
    "_cascade_force_skip_steps/cascade_to_single_dependent.test.sql",
    "_cascade_force_skip_steps/does_not_archive_preexisting_skipped_step_messages.test.sql",
    "_cascade_force_skip_steps/idempotent_second_call.test.sql",
    "_cascade_force_skip_steps/multi_dependency_partial_skip.test.sql",
    "_cascade_force_skip_steps/single_step_skip.test.sql",
    "_cascade_force_skip_steps/skipped_event_payload.test.sql",
    "delete_flow_and_data/drops_queues_by_routes.test.sql"
  ]

  describe "pgTAP parity manifest" do
    @tag :upstream_checkout
    test "lists the 0.17.2 parity files and only Supabase divergences" do
      checkout = System.fetch_env!("PGFLOW_UPSTREAM_CHECKOUT")

      harness =
        "test/support/upstream/manifest.json"
        |> File.read!()
        |> Jason.decode!()
        |> Map.fetch!("harness")

      assert harness["pgtap_parity_files"] == @pgtap_parity_files

      tests_dir = Path.join(checkout, "pkgs/core/supabase/tests")

      Enum.each(@pgtap_parity_files, fn relative ->
        assert File.exists?(Path.join(tests_dir, relative)), "missing #{relative}"
      end)

      assert is_list(harness["known_divergences"])

      Enum.each(harness["known_divergences"], fn entry ->
        assert is_binary(entry["file"])
        assert is_binary(entry["reason"])

        assert entry["reason"] =~ ~r/realtime|pg_net|vault|edge function/,
               "divergence reason must name a Supabase-only dependency: #{entry["reason"]}"
      end)
    end
  end

  describe "mixed workers" do
    @describetag :upstream_checkout

    setup do
      checkout = System.fetch_env!("PGFLOW_UPSTREAM_CHECKOUT")
      fixture = UpstreamHarness.setup_compat_database!()
      System.put_env("PGFLOW_UPSTREAM_CHECKOUT", checkout)

      on_exit(fn -> UpstreamHarness.teardown_compat_database!(fixture) end)

      UpstreamHarness.install_elixir_adaptations!(fixture.database_url)
      %{fixture: fixture, checkout: checkout}
    end

    test "typescript and elixir workers share one flow-mode flow", %{
      fixture: fixture,
      checkout: checkout
    } do
      assert {:ok, result} =
               UpstreamInterop.run_mixed_workers!(checkout, fixture.database_url)

      assert result.ts_status in ["compiled", "verified"]
      assert result.elixir_status in ["compiled", "verified"]
      assert result.completed_runs == 20
      assert length(result.worker_ids) == 2

      ts_version =
        checkout
        |> Path.join("pkgs/edge-worker/package.json")
        |> File.read!()
        |> Jason.decode!()
        |> Map.fetch!("version")

      elixir_version = "elixir-" <> to_string(Application.spec(:pgflow, :vsn))

      assert ts_version in result.versions
      assert elixir_version in result.versions
      assert result.max_attempts == 1
    end
  end

  describe "cross-language execution" do
    @describetag :upstream_checkout

    setup do
      checkout = System.fetch_env!("PGFLOW_UPSTREAM_CHECKOUT")
      fixture = UpstreamHarness.setup_compat_database!()
      System.put_env("PGFLOW_UPSTREAM_CHECKOUT", checkout)

      on_exit(fn -> UpstreamHarness.teardown_compat_database!(fixture) end)

      UpstreamHarness.install_elixir_adaptations!(fixture.database_url)
      %{fixture: fixture, checkout: checkout}
    end

    test "typescript worker completes a flow on the elixir-installed schema", %{
      fixture: fixture,
      checkout: checkout
    } do
      result = run_worker!(checkout, fixture.database_url, "worker_completes_flow")
      assert result["status"] == "passed"
    end

    test "typescript retains bigint message ids as decimal strings", %{
      fixture: fixture,
      checkout: checkout
    } do
      result = run_worker!(checkout, fixture.database_url, "bigint_claim")
      assert result["status"] == "passed"
      assert get_in(hd(result["results"]), ["details", "msg_id"]) != nil
    end

    test "concurrent typescript workers for the same flow do not double-complete", %{
      fixture: fixture,
      checkout: checkout
    } do
      result = run_worker!(checkout, fixture.database_url, "concurrent_workers")
      assert result["status"] == "passed"
    end

    test "elixir starts a flow and the typescript worker completes it", %{
      fixture: fixture,
      checkout: checkout
    } do
      slug = UpstreamHarness.manifest()["harness"]["interop_flow_slug"]
      assert :ok = UpstreamInterop.ensure_interop_flow!(fixture.database_url, slug)

      {:ok, conn} = connect(fixture.database_url)

      try do
        {:ok, run_id} = start_flow!(conn, slug, %{"producer" => "elixir", "value" => 1})

        result =
          run_worker!(
            checkout,
            fixture.database_url,
            "worker_existing_flow",
            %{
              "PGFLOW_INTEROP_FLOW_SLUG" => slug,
              "PGFLOW_INTEROP_RUN_ID" => run_id
            }
          )

        assert result["status"] == "passed"

        assert {:ok, %{rows: [[%{"producer" => "elixir", "value" => 2}]]}} =
                 Postgrex.query(
                   conn,
                   "SELECT output FROM pgflow.step_tasks WHERE run_id = $1::text::uuid",
                   [run_id]
                 )

        assert {:ok, %{rows: [["completed"]]}} =
                 Postgrex.query(
                   conn,
                   "SELECT status FROM pgflow.runs WHERE run_id = $1::text::uuid",
                   [
                     run_id
                   ]
                 )
      after
        GenServer.stop(conn)
      end
    end

    @tag :upstream_harness
    test "typescript starts a flow and the elixir otp worker completes it", %{
      fixture: fixture,
      checkout: checkout
    } do
      assert {:ok, result} = UpstreamInterop.run_reverse_interop!(checkout, fixture.database_url)
      assert result.failures == 0
    end

    test "records upstream json false coercion without weakening elixir assertions", %{
      fixture: fixture,
      checkout: checkout
    } do
      result = run_worker!(checkout, fixture.database_url, "json_falsy_probe")
      probe = hd(result["results"])["details"]

      assert result["unresolved"] == 1
      assert result["tests"] == 0
      assert hd(result["results"])["status"] == "unresolved"
      assert Enum.map(probe["probes"], & &1["expected"]) == [false, 0, ""]
      assert Enum.all?(probe["probes"], &(&1["actual"] == nil and &1["unresolved"]))

      {:ok, conn} = connect(fixture.database_url)

      try do
        worker_id = Ecto.UUID.generate()
        flow_slug = "elixir_false_#{System.unique_integer([:positive])}"

        exec!(conn, "SELECT pgflow.create_flow($1)", [flow_slug])
        exec!(conn, "SELECT pgflow.add_step($1, 'emit')", [flow_slug])

        {:ok, %{rows: [[run_id]]}} =
          Postgrex.query(conn, "SELECT run_id FROM pgflow.start_flow($1, '{}'::jsonb)", [
            flow_slug
          ])

        {:ok, %{rows: [[msg_id]]}} =
          Postgrex.query(
            conn,
            """
            SELECT message_id FROM pgflow.step_tasks
            WHERE run_id = $1 AND step_slug = 'emit'
            """,
            [run_id]
          )

        exec!(
          conn,
          "SELECT pgflow_tests.ensure_worker($1, $2::uuid, 'elixir_worker')",
          [flow_slug, Ecto.UUID.dump!(worker_id)]
        )

        assert {:ok, [_claimed]} =
                 start_tasks_via_postgrex(conn, flow_slug, msg_id, worker_id, flow_slug)

        assert {:ok, row} = complete_task_via_postgrex(conn, run_id, "emit", 0, false)
        assert row["status"] == "completed"

        {:ok, %{rows: [[output]]}} =
          Postgrex.query(
            conn,
            "SELECT output FROM pgflow.step_tasks WHERE run_id = $1 AND step_slug = 'emit'",
            [run_id]
          )

        assert output == false
        refute output == nil
      after
        GenServer.stop(conn)
      end
    end
  end

  defp run_worker!(checkout, database_url, scenario, extra_env \\ %{}) do
    script = Path.expand("../../support/upstream/worker.mjs", __DIR__)

    env =
      [
        {"PGFLOW_UPSTREAM_CHECKOUT", checkout},
        {"DATABASE_URL", database_url},
        {"PGFLOW_COMPAT_DATABASE_URL", database_url}
      ] ++ Enum.map(extra_env, fn {k, v} -> {k, v} end)

    node = System.get_env("PGFLOW_NODE") || System.find_executable("node") || "node"
    script_dir = Path.dirname(script)

    {output, 0} =
      System.cmd(node, [script, "--scenario", scenario],
        cd: script_dir,
        env: env,
        stderr_to_stdout: true
      )

    Jason.decode!(output)
  end

  defp start_flow!(conn, slug, input) do
    case Postgrex.query(conn, "SELECT run_id::text FROM pgflow.start_flow($1, $2::jsonb)", [
           slug,
           input
         ]) do
      {:ok, %{rows: [[run_id]]}} -> {:ok, run_id}
      {:error, error} -> {:error, error}
    end
  end

  defp connect(database_url) do
    uri = URI.parse(database_url)
    [username, password] = String.split(uri.userinfo || "", ":", parts: 2)

    Postgrex.start_link(
      hostname: uri.host,
      port: uri.port || 5432,
      username: URI.decode(username),
      password: URI.decode(password),
      database: String.trim_leading(uri.path, "/")
    )
  end

  defp exec!(conn, sql, params) do
    {:ok, _} = Postgrex.query(conn, sql, params)
  end

  defp start_tasks_via_postgrex(conn, flow_slug, msg_id, worker_id, queue_name) do
    sql = "SELECT row_to_json(t) FROM pgflow.start_tasks($1, $2::bigint[], $3::uuid, $4) AS t"

    case Postgrex.query(conn, sql, [flow_slug, [msg_id], Ecto.UUID.dump!(worker_id), queue_name]) do
      {:ok, %{rows: rows}} ->
        {:ok, Enum.map(rows, fn [json] -> json end)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp complete_task_via_postgrex(conn, run_id, step_slug, task_index, output) do
    sql = "SELECT row_to_json(t) FROM pgflow.complete_task($1::uuid, $2, $3, $4::jsonb) AS t"

    case Postgrex.query(conn, sql, [run_id, step_slug, task_index, output]) do
      {:ok, %{rows: [[row]]}} -> {:ok, row}
      {:error, error} -> {:error, error}
    end
  end
end
