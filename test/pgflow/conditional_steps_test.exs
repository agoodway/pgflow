defmodule PgFlow.ConditionalStepsTest do
  use PgFlow.IntegrationCase

  @moduletag :integration

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
