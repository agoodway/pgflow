defmodule PgFlow.ConditionalStepsTest do
  use PgFlow.IntegrationCase

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Client
  alias PgFlow.Queries.Flows
  alias PgFlow.Worker.Server

  @moduletag :integration

  defmodule CondAnalyzeFlow do
    use PgFlow.Flow
    @flow slug: :cond_analyze_flow, max_attempts: 1

    step :analyze do
      fn _input, _ctx -> %{"flag" => false} end
    end

    step :moderate,
      depends_on: [:analyze],
      if: %{analyze: %{flag: true}},
      when_unmet: :skip do
      fn deps, _ctx -> %{"ok" => deps.analyze} end
    end
  end

  defmodule CondEmailFlow do
    use PgFlow.Flow
    @flow slug: :cond_email_flow, max_attempts: 1, base_delay: 1

    step :email, max_attempts: 1, when_exhausted: :skip do
      fn _input, _ctx -> raise "smtp timeout" end
    end

    step :finish, depends_on: [:email] do
      fn _deps, _ctx -> %{"ok" => true} end
    end
  end

  defmodule CondFailFastFlow do
    use PgFlow.Flow
    @flow slug: :cond_fail_fast_flow, max_attempts: 1

    # No conditions anywhere in this flow - the root handler just raises.
    # Used to prove emit_post_start never mislabels a genuine handler
    # failure as "condition unmet".
    step :boom do
      fn _input, _ctx -> raise "handler exploded" end
    end
  end

  defmodule CondChainFlow do
    use PgFlow.Flow
    @flow slug: :cond_chain_flow, max_attempts: 1

    step :analyze do
      fn _input, _ctx -> %{"flag" => false} end
    end

    step :moderate,
      depends_on: [:analyze],
      if: %{analyze: %{flag: true}},
      when_unmet: :skip do
      fn deps, _ctx -> deps end
    end

    step :one, depends_on: [:analyze] do
      fn _deps, _ctx -> %{"ok" => 1} end
    end

    step :two, depends_on: [:one] do
      fn _deps, _ctx -> %{"ok" => 2} end
    end

    step :three, depends_on: [:two] do
      fn _deps, _ctx -> %{"ok" => 3} end
    end
  end

  defmodule CondStaleMapFlow do
    use PgFlow.Flow
    @flow slug: :cond_stale_map_flow, max_attempts: 1, base_delay: 1

    step :fanout do
      fn _input, _ctx -> [1, 2, 3] end
    end

    # One map task per element. The first task to run exhausts its single
    # attempt, which skips the whole step - while its two siblings still have
    # live pgmq messages.
    map :items, array: :fanout, max_attempts: 1, when_exhausted: :skip do
      fn item, _ctx ->
        PgFlow.ConditionalStepsTest.report_handler_call({:item_handled, item})
        raise "item #{item} exploded"
      end
    end
  end

  defmodule CondStaleSingleFlow do
    use PgFlow.Flow
    @flow slug: :cond_stale_single_flow, max_attempts: 1

    step :only do
      fn _input, _ctx ->
        PgFlow.ConditionalStepsTest.report_handler_call(:only_handled)
        %{"ok" => true}
      end
    end
  end

  @probe_name :pgflow_stale_skip_probe

  # Handlers run inside Task.Supervisor children, so they report back through a
  # registered name rather than a captured pid.
  def report_handler_call(message) do
    case Process.whereis(@probe_name) do
      nil -> :ok
      pid -> send(pid, message)
    end
  end

  describe "schema" do
    test "steps table has 0.14 condition columns" do
      %{rows: rows} =
        TestRepo.query!("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pgflow' AND table_name = 'steps'
          AND column_name IN (
            'required_input_pattern', 'forbidden_input_pattern',
            'when_unmet', 'when_exhausted'
          )
        ORDER BY column_name
        """)

      assert Enum.map(rows, &hd/1) == [
               "forbidden_input_pattern",
               "required_input_pattern",
               "when_exhausted",
               "when_unmet"
             ]
    end
  end

  describe "if / when_unmet skip" do
    test "root step is skipped when if pattern is unmet" do
      create_flow("cond_root_skip")

      add_conditional_step("cond_root_skip", "premium_only",
        if: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_root_skip", %{"plan" => "free"})
      states = get_step_states(run_id)

      assert [%{step_slug: "premium_only", status: "skipped", skip_reason: "condition_unmet"}] =
               states

      assert get_run_status(run_id) == "completed"
      assert get_step_tasks(run_id) == []
    end
  end

  describe "if_not" do
    test "root step runs when forbidden pattern is absent" do
      create_flow("cond_if_not")

      add_conditional_step("cond_if_not", "trial",
        if_not: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_if_not", %{"plan" => "free"})
      [state] = get_step_states(run_id)
      assert state.status in ["created", "started", "completed"]
      refute state.status == "skipped"
    end

    test "root step skips when forbidden pattern is present" do
      create_flow("cond_if_not_skip")

      add_conditional_step("cond_if_not_skip", "trial",
        if_not: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_if_not_skip", %{"plan" => "premium"})
      [state] = get_step_states(run_id)
      assert state.status == "skipped"
      assert state.skip_reason == "condition_unmet"
    end
  end

  describe "non-cascade skip feeding a running dependent" do
    test "dependent runs and its input omits the skipped dep's key entirely" do
      create_flow("cond_omit_key")

      add_conditional_step("cond_omit_key", "a",
        if: %{"go" => true},
        when_unmet: "skip"
      )

      add_step("cond_omit_key", "b", deps: ["a"])

      run_id = start_flow_run("cond_omit_key", %{"go" => false})

      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["a"].status == "skipped"
      assert states["a"].skip_reason == "condition_unmet"
      # A plain (non-cascade) skip only satisfies the dependency edge - it
      # does not propagate the skip to "b". "b" must actually run.
      assert states["b"].status == "started"

      [task] = read_and_start("cond_omit_key", vt: 1, qty: 1)
      assert task.step_slug == "b"

      # Upstream (dsl.ts) semantics: a skipped, non-cascade dependency's key
      # is OMITTED from the dependent's input map - not present, and
      # specifically not present-with-null. Verified directly against the
      # vendored SQL's start_tasks: the `deps` CTE inner-joins step_states on
      # `status = 'completed'`, so a skipped dep never contributes a key to
      # jsonb_object_agg, and coalesce(..., '{}'::jsonb) is what "b" actually
      # receives here - confirming the vendored SQL matches upstream (no
      # divergence to pin).
      refute Map.has_key?(task.input, "a")
      assert task.input == %{}
    end
  end

  describe "when_unmet modes" do
    test "fail fails the run" do
      create_flow("cond_fail")

      add_conditional_step("cond_fail", "need_premium",
        if: %{"plan" => "premium"},
        when_unmet: "fail"
      )

      run_id = start_flow_run("cond_fail", %{"plan" => "free"})
      assert get_run_status(run_id) == "failed"
    end

    test "skip-cascade skips dependents" do
      create_flow("cond_cascade")

      add_conditional_step("cond_cascade", "load",
        if: %{"plan" => "premium"},
        when_unmet: "skip-cascade"
      )

      add_step("cond_cascade", "perk", deps: ["load"])
      add_step("cond_cascade", "finish")

      run_id = start_flow_run("cond_cascade", %{"plan" => "free"})
      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})

      assert states["load"].status == "skipped"
      assert states["perk"].status == "skipped"
      assert states["perk"].skip_reason == "dependency_skipped"
      assert states["finish"].status != "skipped"
    end

    test "skip-cascade fans out to every direct dependent" do
      create_flow("cond_cascade_fanout")

      add_conditional_step("cond_cascade_fanout", "load",
        if: %{"plan" => "premium"},
        when_unmet: "skip-cascade"
      )

      add_step("cond_cascade_fanout", "perk_a", deps: ["load"])
      add_step("cond_cascade_fanout", "perk_b", deps: ["load"])

      run_id = start_flow_run("cond_cascade_fanout", %{"plan" => "free"})
      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})

      assert states["load"].status == "skipped"
      assert states["load"].skip_reason == "condition_unmet"
      assert states["perk_a"].status == "skipped"
      assert states["perk_a"].skip_reason == "dependency_skipped"
      assert states["perk_b"].status == "skipped"
      assert states["perk_b"].skip_reason == "dependency_skipped"

      assert get_run_status(run_id) == "completed"
    end

    test "skip-cascade walks multiple levels" do
      create_flow("cond_cascade_deep")

      add_conditional_step("cond_cascade_deep", "a",
        if: %{"on" => true},
        when_unmet: "skip-cascade"
      )

      add_step("cond_cascade_deep", "b", deps: ["a"])
      add_step("cond_cascade_deep", "c", deps: ["b"])

      run_id = start_flow_run("cond_cascade_deep", %{"on" => false})
      slugs = get_step_states(run_id) |> Enum.map(& &1.status)
      assert slugs == ["skipped", "skipped", "skipped"]
    end
  end

  describe "dependent if" do
    test "matches against {dep_slug => output}" do
      create_flow("cond_dep_if")
      add_step("cond_dep_if", "analyze")

      add_conditional_step("cond_dep_if", "moderate",
        deps: ["analyze"],
        if: %{"analyze" => %{"needs_moderation" => true}},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_dep_if", %{})
      poll_and_complete_with_output("cond_dep_if", %{"needs_moderation" => false})

      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["moderate"].status == "skipped"
    end
  end

  describe "if AND if_not" do
    test "both must be satisfied" do
      create_flow("cond_and")

      add_conditional_step("cond_and", "std",
        if: %{"status" => "active"},
        if_not: %{"role" => "admin"},
        when_unmet: "skip"
      )

      skipped = start_flow_run("cond_and", %{"status" => "active", "role" => "admin"})
      assert hd(get_step_states(skipped)).status == "skipped"

      create_flow("cond_and2")

      add_conditional_step("cond_and2", "std",
        if: %{"status" => "active"},
        if_not: %{"role" => "admin"},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_and2", %{"status" => "active", "role" => "user"})
      refute hd(get_step_states(run_id)).status == "skipped"
    end
  end

  describe "when_exhausted" do
    test "skip continues the run after retries" do
      create_flow_with_options("cond_exh", max_attempts: 1)
      add_conditional_step("cond_exh", "email", when_exhausted: "skip", max_attempts: 1)
      add_step("cond_exh", "account", deps: ["email"])

      run_id = start_flow_run("cond_exh", %{})
      fail_until_terminal("cond_exh", run_id, "email")

      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["email"].status == "skipped"
      assert states["email"].skip_reason == "handler_failed"
      refute get_run_status(run_id) == "failed"
    end

    test "TYPE_VIOLATION still fails the run" do
      create_flow("cond_type")
      add_conditional_step("cond_type", "items", when_exhausted: "skip")
      add_step("cond_type", "each", deps: ["items"], type: "map")

      run_id = start_flow_run("cond_type", %{})
      poll_and_complete_with_output("cond_type", "not-an-array")

      assert get_run_status(run_id) == "failed"
    end
  end

  describe "map skip" do
    test "skipped map creates no per-item tasks" do
      create_flow("cond_map")
      # Live start_flow rejects object input for root maps. A dependent map is
      # skipped after its parent completes, which still proves no per-item tasks.
      add_step("cond_map", "items")

      add_conditional_step("cond_map", "each",
        type: "map",
        deps: ["items"],
        if: %{"include" => true},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_map", %{})
      poll_and_complete_with_output("cond_map", [1, 2])

      assert get_step_tasks_for_step(run_id, "each") == []
      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["each"].status == "skipped"
    end

    test "condition met + empty-array input completes with 0 items (not skipped, not hung)" do
      create_flow("cond_map_empty_met")
      add_step("cond_map_empty_met", "items")

      add_conditional_step("cond_map_empty_met", "each",
        type: "map",
        deps: ["items"],
        if: %{"items" => []},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_map_empty_met", %{})
      poll_and_complete_with_output("cond_map_empty_met", [])

      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["each"].status == "completed"
      refute states["each"].skip_reason
      assert get_step_output(run_id, "each") == []
      assert get_step_tasks_for_step(run_id, "each") == []
      assert get_run_status(run_id) == "completed"
    end

    test "condition unmet + empty-array input skips (distinguishable from the completed case)" do
      create_flow("cond_map_empty_unmet")
      add_step("cond_map_empty_unmet", "items")

      add_conditional_step("cond_map_empty_unmet", "each",
        type: "map",
        deps: ["items"],
        if: %{"other" => true},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_map_empty_unmet", %{})
      poll_and_complete_with_output("cond_map_empty_unmet", [])

      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["each"].status == "skipped"
      assert states["each"].skip_reason == "condition_unmet"
      assert get_step_output(run_id, "each") == nil
      assert get_step_tasks_for_step(run_id, "each") == []
      assert get_run_status(run_id) == "completed"
    end
  end

  describe "all-skipped multi-step run" do
    test "run completes when every step is skipped via mixed unmet-if and cascade" do
      create_flow("cond_all_skipped")

      add_conditional_step("cond_all_skipped", "solo",
        if: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      add_conditional_step("cond_all_skipped", "load",
        if: %{"plan" => "premium"},
        when_unmet: "skip-cascade"
      )

      add_step("cond_all_skipped", "perk", deps: ["load"])

      run_id = start_flow_run("cond_all_skipped", %{"plan" => "free"})
      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})

      assert states["solo"].status == "skipped"
      assert states["solo"].skip_reason == "condition_unmet"
      assert states["load"].status == "skipped"
      assert states["load"].skip_reason == "condition_unmet"
      assert states["perk"].status == "skipped"
      assert states["perk"].skip_reason == "dependency_skipped"

      assert get_run_status(run_id) == "completed"
      assert get_step_tasks(run_id) == []
    end
  end

  describe "Queries.Flows.list_skipped_steps/2" do
    test "returns skipped slugs and reasons" do
      create_flow("cond_list_skip")

      add_conditional_step("cond_list_skip", "only",
        if: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      run_id = start_flow_run("cond_list_skip", %{"plan" => "free"})

      assert {:ok, [%{step_slug: "only", skip_reason: "condition_unmet"}]} =
               Flows.list_skipped_steps(TestRepo, run_id)
    end

    test "orders cascade ties topologically, not alphabetically" do
      create_flow("cond_list_order")

      # A cascade writes one `skipped_at = now()` for every step in the chain,
      # so skipped_at always ties. Names are deliberately reverse-alphabetical
      # to the dependency order: parent "zeta" -> child "alpha".
      add_conditional_step("cond_list_order", "zeta",
        if: %{"go" => true},
        when_unmet: "skip-cascade"
      )

      add_step("cond_list_order", "alpha", deps: ["zeta"])

      run_id = start_flow_run("cond_list_order", %{"go" => false})

      assert {:ok, [%{step_slug: "zeta"}, %{step_slug: "alpha"}]} =
               Flows.list_skipped_steps(TestRepo, run_id)
    end
  end

  describe "Client.start_flow emits skips" do
    test "root-only skip emits step:skipped and run:completed without a worker" do
      create_flow("cond_client_skip")

      add_conditional_step("cond_client_skip", "premium",
        if: %{"plan" => "premium"},
        when_unmet: "skip"
      )

      :persistent_term.put({PgFlow, :repo}, TestRepo)
      on_exit(fn -> :persistent_term.erase({PgFlow, :repo}) end)

      self = self()

      :telemetry.attach_many(
        "cond-client-skip",
        [[:pgflow, :step, :skipped], [:pgflow, :run, :completed]],
        fn event, _m, meta, _ -> send(self, {event, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("cond-client-skip") end)

      assert {:ok, run_id} =
               Client.start_flow("cond_client_skip", %{"plan" => "free"})

      assert_receive {[:pgflow, :step, :skipped], meta}
      assert meta.step_slug == "premium"
      assert meta.skip_reason == "condition_unmet"
      assert meta.run_id == run_id

      assert_receive {[:pgflow, :run, :completed], %{run_id: ^run_id}}
    end

    test "cascade skips arrive parent before child" do
      create_flow("cond_order_skip")

      add_conditional_step("cond_order_skip", "zeta",
        if: %{"go" => true},
        when_unmet: "skip-cascade"
      )

      add_step("cond_order_skip", "alpha", deps: ["zeta"])

      :persistent_term.put({PgFlow, :repo}, TestRepo)
      on_exit(fn -> :persistent_term.erase({PgFlow, :repo}) end)

      self = self()

      :telemetry.attach(
        "cond-order-skip",
        [:pgflow, :step, :skipped],
        fn event, _m, meta, _ -> send(self, {event, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach("cond-order-skip") end)

      assert {:ok, run_id} = Client.start_flow("cond_order_skip", %{"go" => false})

      assert_receive {[:pgflow, :step, :skipped], first}
      assert_receive {[:pgflow, :step, :skipped], second}

      assert first.run_id == run_id
      assert first.step_slug == "zeta"
      assert second.step_slug == "alpha"
    end

    test "when_unmet: :fail decided synchronously still emits run:failed with a truthful reason" do
      create_flow("cond_client_fail")

      add_conditional_step("cond_client_fail", "need_premium",
        if: %{"plan" => "premium"},
        when_unmet: "fail"
      )

      :persistent_term.put({PgFlow, :repo}, TestRepo)
      on_exit(fn -> :persistent_term.erase({PgFlow, :repo}) end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :run, :failed]
        ])

      assert {:ok, run_id} = Client.start_flow("cond_client_fail", %{"plan" => "free"})

      assert_receive {[:pgflow, :run, :failed], ^ref, _m, metadata}
      assert metadata.run_id == run_id
      assert metadata.flow_slug == "cond_client_fail"
      assert is_binary(metadata.error)
      assert metadata.error =~ "condition"
    end
  end

  describe "worker notices skips after complete/fail" do
    setup do
      Sandbox.mode(TestRepo, :auto)
      TestRepo.query!("SELECT pgflow_tests.reset_db()")
      :persistent_term.put({PgFlow, :repo}, TestRepo)
      {:ok, task_supervisor} = Task.Supervisor.start_link()

      on_exit(fn ->
        :persistent_term.erase({PgFlow, :repo})
        Sandbox.mode(TestRepo, :manual)
      end)

      compile_definition(CondAnalyzeFlow)
      compile_definition(CondEmailFlow)
      compile_definition(CondChainFlow)
      {:ok, task_supervisor: task_supervisor}
    end

    test "emits step:skipped once, not once per later completion", %{
      task_supervisor: task_supervisor
    } do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :step, :skipped],
          [:pgflow, :run, :completed]
        ])

      worker_pid = start_worker(CondChainFlow, task_supervisor)
      {:ok, run_id} = Client.start_flow(CondChainFlow, %{})
      wait_for_run_completion(run_id)

      assert_receive {[:pgflow, :step, :skipped], ^ref, _m,
                      %{step_slug: "moderate", run_id: ^run_id}},
                     5_000

      # `analyze` skips `moderate`; `one`/`two`/`three` then complete in sequence.
      # Every one of those completions re-lists the run's skipped steps, so a
      # non-delta emitter re-announces `moderate` three more times. run:completed
      # is emitted after the final completion's skip sweep, so by the time it
      # arrives any duplicate is already in the mailbox.
      assert_receive {[:pgflow, :run, :completed], ^ref, _m, %{run_id: ^run_id}}, 5_000
      refute_received {[:pgflow, :step, :skipped], ^ref, _, %{step_slug: "moderate"}}

      Server.stop(worker_pid)
    end

    test "emits step:skipped when a dependent is skipped after complete", %{
      task_supervisor: task_supervisor
    } do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :step, :skipped]
        ])

      worker_pid = start_worker(CondAnalyzeFlow, task_supervisor)
      {:ok, run_id} = Client.start_flow(CondAnalyzeFlow, %{})
      wait_for_run_completion(run_id)

      assert_receive {[:pgflow, :step, :skipped], ^ref, _m,
                      %{step_slug: "moderate", skip_reason: "condition_unmet", run_id: ^run_id}},
                     5_000

      Server.stop(worker_pid)
    end

    test "when_exhausted skip does not emit run:failed", %{task_supervisor: task_supervisor} do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :step, :skipped],
          [:pgflow, :run, :failed]
        ])

      worker_pid = start_worker(CondEmailFlow, task_supervisor)
      {:ok, run_id} = Client.start_flow(CondEmailFlow, %{})
      wait_for_run_completion(run_id)

      assert_receive {[:pgflow, :step, :skipped], ^ref, _m,
                      %{step_slug: "email", skip_reason: "handler_failed"}},
                     5_000

      refute_received {[:pgflow, :run, :failed], ^ref, _, _}
      Server.stop(worker_pid)
    end
  end

  describe "emit_post_start reports the observed transition, not a re-read" do
    setup do
      Sandbox.mode(TestRepo, :auto)
      TestRepo.query!("SELECT pgflow_tests.reset_db()")
      :persistent_term.put({PgFlow, :repo}, TestRepo)
      {:ok, task_supervisor} = Task.Supervisor.start_link()

      on_exit(fn ->
        :persistent_term.erase({PgFlow, :repo})
        Sandbox.mode(TestRepo, :manual)
      end)

      compile_definition(CondFailFastFlow)
      {:ok, task_supervisor: task_supervisor}
    end

    test "a root handler failing fast is never mislabeled as condition unmet", %{
      task_supervisor: task_supervisor
    } do
      # Nothing conditional anywhere in this flow. A worker with an
      # aggressive poll interval is started before the run even exists, so
      # it is racing to fail the run's only task the instant it becomes
      # visible - the exact window the old `Flows.get_run` re-read in
      # `emit_post_start` was vulnerable to.
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :run, :failed]
        ])

      worker_pid = start_fast_worker(CondFailFastFlow, task_supervisor)
      {:ok, run_id} = Client.start_flow(CondFailFastFlow, %{})
      wait_for_run_completion(run_id)

      assert_receive {[:pgflow, :run, :failed], ^ref, _m, metadata}, 5_000
      assert metadata.run_id == run_id
      assert is_binary(metadata.error)
      refute metadata.error == "condition unmet"
      assert metadata.error =~ "handler exploded"

      # The worker owns this run's terminal event; the client must not have
      # also emitted one from a racing re-read.
      refute_received {[:pgflow, :run, :failed], ^ref, _, _}

      Server.stop(worker_pid)
    end
  end

  describe "stale queue messages for skipped steps" do
    setup do
      Sandbox.mode(TestRepo, :auto)
      TestRepo.query!("SELECT pgflow_tests.reset_db()")
      :persistent_term.put({PgFlow, :repo}, TestRepo)
      {:ok, task_supervisor} = Task.Supervisor.start_link()
      Process.register(self(), @probe_name)

      on_exit(fn ->
        :persistent_term.erase({PgFlow, :repo})
        Sandbox.mode(TestRepo, :manual)
      end)

      compile_definition(CondStaleMapFlow)
      compile_definition(CondStaleSingleFlow)
      {:ok, task_supervisor: task_supervisor}
    end

    test "a skip landing on live sibling messages never reaches a handler", %{
      task_supervisor: task_supervisor
    } do
      # `items` fans out to three tasks, so three messages are in the queue when
      # task 0 exhausts its only attempt. `fail_task` skips the whole step and
      # archives its siblings' still-queued messages in the same transaction.
      worker_pid = start_serial_worker(CondStaleMapFlow, task_supervisor)
      {:ok, run_id} = Client.start_flow(CondStaleMapFlow, %{})

      assert_receive {:item_handled, 1}, 5_000
      wait_for_run_completion(run_id)

      # The siblings' messages were archived before any worker could read them.
      refute_received {:item_handled, 2}
      refute_received {:item_handled, 3}

      states = Map.new(get_step_states(run_id), &{&1.step_slug, &1})
      assert states["items"].status == "skipped"
      assert states["items"].skip_reason == "handler_failed"

      assert queued_message_count("cond_stale_map_flow") == 0
      assert archived_task_indices("cond_stale_map_flow", "items") == [0, 1, 2]

      assert Process.alive?(worker_pid)
      Server.stop(worker_pid)
    end

    test "a message read after its step was skipped is refused, archived, and never dispatched",
         %{task_supervisor: task_supervisor} do
      # Harsher than production: the step is skipped while its message stays
      # visible, so the worker really does read a message for a skipped step.
      # (Every SQL skip path archives such messages, so a worker normally holds
      # this message only for the microseconds between reading it and calling
      # start_tasks.) `start_tasks` refuses to return a task whose step_state is
      # not 'started', so nothing is dispatched — and the archive-invariant
      # tripwire then archives the orphaned message itself, instead of leaving
      # it to redeliver and be refused again forever.
      {:ok, run_id} = Client.start_flow(CondStaleSingleFlow, %{})
      wait_until(fn -> queued_message_count("cond_stale_single_flow") == 1 end)
      skip_step_leaving_message_visible(run_id, "only")

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:pgflow, :worker, :poll, :stop],
          [:pgflow, :worker, :task, :start]
        ])

      worker_pid = start_serial_worker(CondStaleSingleFlow, task_supervisor)

      # Three completed poll cycles is more than enough for the worker to have
      # read the visible message and decided what to do with it.
      for _ <- 1..3 do
        assert_receive {[:pgflow, :worker, :poll, :stop], ^ref, _m, _meta}, 5_000
      end

      refute_received {[:pgflow, :worker, :task, :start], ^ref, _, _}
      refute_received :only_handled

      # The tripwire moved the orphan out of the live queue; its read_ct in
      # the archive proves the worker read it (rather than SQL archiving an
      # unread message).
      assert queued_message_count("cond_stale_single_flow") == 0
      assert archived_message_read_count("cond_stale_single_flow") > 0

      assert Process.alive?(worker_pid)
      Server.stop(worker_pid)
    end
  end

  # max_concurrency 1 makes each poll read exactly one message, so the map
  # step's tasks are handed out one at a time instead of all in one batch.
  defp start_serial_worker(flow_module, task_supervisor) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: 1,
      batch_size: 1,
      signal_strategy: :polling,
      min_poll_interval: 20,
      max_poll_interval: 100,
      notify_fallback_interval: 30_000
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  # Skips a started step the way SQL does, minus the message archiving, to leave
  # a stale-but-visible message behind for the worker to read.
  defp skip_step_leaving_message_visible(run_id, step_slug) do
    TestRepo.query!(
      """
      UPDATE pgflow.step_states
      SET status = 'skipped',
          skip_reason = 'condition_unmet',
          skipped_at = now(),
          remaining_tasks = NULL
      WHERE run_id = $1 AND step_slug = $2
      """,
      [Ecto.UUID.dump!(run_id), step_slug]
    )
  end

  defp get_step_output(run_id, step_slug) do
    %{rows: [[output]]} =
      TestRepo.query!(
        "SELECT output FROM pgflow.step_states WHERE run_id = $1 AND step_slug = $2",
        [Ecto.UUID.dump!(run_id), step_slug]
      )

    output
  end

  defp queued_message_count(queue_name) do
    %{rows: [[count]]} = TestRepo.query!("SELECT count(*) FROM pgmq.q_#{queue_name}")
    count
  end

  defp archived_message_read_count(queue_name) do
    %{rows: [[read_ct]]} =
      TestRepo.query!("SELECT coalesce(max(read_ct), 0) FROM pgmq.a_#{queue_name}")

    read_ct
  end

  defp archived_task_indices(queue_name, step_slug) do
    %{rows: rows} =
      TestRepo.query!(
        """
        SELECT (message->>'task_index')::int
        FROM pgmq.a_#{queue_name}
        WHERE message->>'step_slug' = $1
        ORDER BY 1
        """,
        [step_slug]
      )

    Enum.map(rows, &hd/1)
  end

  defp compile_definition(module) do
    Enum.each(PgFlow.FlowCompiler.compile(module.__pgflow_definition__()), fn sql ->
      TestRepo.query!(sql)
    end)
  end

  defp start_worker(flow_module, task_supervisor) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: 10,
      batch_size: 10,
      signal_strategy: :polling,
      min_poll_interval: 50,
      max_poll_interval: 5_000,
      notify_fallback_interval: 30_000
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  # Polls as aggressively as the worker allows, to maximize the odds of
  # racing a run's task to completion before any other reader of the run
  # observes it - used to stress the exact window `emit_post_start` used to
  # be vulnerable to.
  defp start_fast_worker(flow_module, task_supervisor) do
    config = %{
      flow_module: flow_module,
      repo: TestRepo,
      task_supervisor: task_supervisor,
      max_concurrency: 10,
      batch_size: 10,
      signal_strategy: :polling,
      min_poll_interval: 1,
      max_poll_interval: 5,
      notify_fallback_interval: 30_000
    }

    {:ok, pid} = Server.start_link(config)
    Sandbox.allow(TestRepo, self(), pid)
    pid
  end

  # poll_and_fail/1 may not exhaust max_attempts: 1 in one call if the task
  # is requeued. Loop until the step is terminal (or fail_task if already started).
  defp fail_until_terminal(flow_slug, run_id, step_slug, attempts_left \\ 5) do
    state = Enum.find(get_step_states(run_id), &(&1.step_slug == step_slug))

    if state.status in ["skipped", "failed", "completed"] do
      :ok
    else
      if attempts_left == 0 do
        flunk("step #{step_slug} did not become terminal after repeated fail_task/poll_and_fail")
      end

      case get_task_details(run_id, step_slug, 0) do
        %{status: "started"} ->
          fail_task(run_id, step_slug, 0, "#{step_slug} FAILED")

        _ ->
          poll_and_fail(flow_slug)
      end

      fail_until_terminal(flow_slug, run_id, step_slug, attempts_left - 1)
    end
  end
end
