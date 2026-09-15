defmodule PgflowDemo.ScenarioRunnerTest do
  use PgflowDemo.ScenarioCase, async: false

  alias PgFlow.Runs
  alias PgflowDemo.{ScenarioControls, ScenarioRunner, Scenarios}

  @moduletag :integration
  @moduletag :integration_sandbox

  setup_all do
    setup_integration_sandbox()
    :ok
  end

  describe "start/2 and load/1" do
    test "starts a deterministic retry scenario and persists attempts" do
      assert {:ok, run_id} = ScenarioRunner.start("retry", "succeed_on_third_attempt")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, tasks} = Runs.list_run_tasks(Repo, run_id)
      task = Enum.find(tasks, &(&1.step_slug == "retry_step"))
      assert task.attempts_count == 3
      assert task.status == "completed"

      assert {:ok, loaded} = ScenarioRunner.load(run_id)
      assert loaded.run_id == run_id
      assert loaded.scenario_id == "retry"
      assert loaded.preset == "succeed_on_third_attempt"
    end

    test "returns unknown scenario error" do
      assert {:error, :unknown_scenario} = ScenarioRunner.start("missing", "any")
    end

    test "returns unknown preset error" do
      assert {:error, :unknown_preset} = ScenarioRunner.start("retry", "missing_preset")
    end

    test "record job preset completes with payload echo" do
      assert {:ok, run_id} = ScenarioRunner.start("record_job", "immediate_echo")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["record_job", "payload"]) == %{"message" => "hello"}
    end

    test "record job delayed enqueue preset completes with payload echo" do
      assert {:ok, run_id} = ScenarioRunner.start("record_job", "delayed_enqueue")

      run = await_terminal_run(run_id, timeout_ms: 20_000)
      assert run.status == "completed"
      assert get_in(run.output, ["record_job", "payload"]) == %{"message" => "later"}
    end

    test "json preset preserves false and null" do
      assert {:ok, run_id} = ScenarioRunner.start("json", "false_and_null")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, states} = Runs.list_step_states(Repo, run_id)
      output = step_output(states, "emit")
      assert output["flag"] == false
      assert is_nil(output["empty"])
    end

    test "json presets preserve scalar, list, and object values" do
      for {preset, key, expected} <- [
            {"scalar_value", "scalar", 42},
            {"list_values", "list", [1, "a", false]},
            {"object_values", "object", %{"nested" => true, "count" => 2}}
          ] do
        assert {:ok, run_id} = ScenarioRunner.start("json", preset)

        run = await_terminal_run(run_id)
        assert run.status == "completed"

        {:ok, states} = Runs.list_step_states(Repo, run_id)
        output = step_output(states, "emit")
        assert output[key] == expected
      end
    end

    test "scheduled flow manual tick completes without installing a cron schedule" do
      assert {:ok, descriptor} = Scenarios.fetch("scheduled")
      job_name = "pgflow:#{descriptor.flow_slug}"
      count_before = cron_job_count(job_name)

      assert {:ok, run_id} = ScenarioRunner.start("scheduled", "manual_tick")
      run = await_terminal_run(run_id, timeout_ms: 20_000)
      assert run.status == "completed"

      assert cron_job_count(job_name) == count_before
    end
  end

  describe "executable scenario matrix" do
    test "parallel default_fan_in persists fan-in sum" do
      assert {:ok, run_id} = ScenarioRunner.start("parallel", "default_fan_in")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["merge", "sum"]) == 10
    end

    test "map three_items persists aggregate total" do
      assert {:ok, run_id} = ScenarioRunner.start("map", "three_items")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["aggregate", "total"]) == 12
    end

    test "root_map normal_list persists child summary" do
      assert {:ok, run_id} = ScenarioRunner.start("root_map", "normal_list")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["summarize", "count"]) == 3
      assert get_in(run.output, ["summarize", "items"]) == [1, 2, 3]
    end

    test "onboarding premium_ok persists premium branch" do
      assert {:ok, run_id} = ScenarioRunner.start("onboarding", "premium_ok")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, states} = Runs.list_step_states(Repo, run_id)
      assert step_status(states, "setup_premium") == "completed"
      assert step_status(states, "activate_perk") == "completed"
      assert get_in(run.output, ["finish", "ok"]) == true
    end

    test "policy_if_met default runs conditional step" do
      assert {:ok, run_id} = ScenarioRunner.start("policy_if_met", "default")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["conditional", "ran"]) == true
    end

    test "policy_if_not default runs conditional step" do
      assert {:ok, run_id} = ScenarioRunner.start("policy_if_not", "default")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["conditional", "ran"]) == true
    end

    test "policy_when_unmet_skip default skips gated step" do
      assert {:ok, run_id} = ScenarioRunner.start("policy_when_unmet_skip", "default")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, states} = Runs.list_step_states(Repo, run_id)
      assert step_status(states, "gated") == "skipped"
      assert get_in(run.output, ["after_gate", "after"]) == false
    end

    test "policy_when_unmet_skip_cascade default skips downstream" do
      assert {:ok, run_id} = ScenarioRunner.start("policy_when_unmet_skip_cascade", "default")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, states} = Runs.list_step_states(Repo, run_id)
      assert step_status(states, "gated") == "skipped"
      assert step_status(states, "downstream") == "skipped"
    end

    test "policy_when_unmet_fail default fails run" do
      assert {:ok, run_id} = ScenarioRunner.start("policy_when_unmet_fail", "default")

      run = await_terminal_run(run_id)
      assert run.status == "failed"
    end

    test "exhaustion_fail default fails run" do
      assert {:ok, run_id} = ScenarioRunner.start("exhaustion_fail", "default")

      run = await_terminal_run(run_id)
      assert run.status == "failed"
    end

    test "exhaustion_skip default completes after skip" do
      assert {:ok, run_id} = ScenarioRunner.start("exhaustion_skip", "default")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, states} = Runs.list_step_states(Repo, run_id)
      assert step_status(states, "fail_soft") == "skipped"
      assert get_in(run.output, ["finish", "continued"]) == true
    end

    test "exhaustion_skip_cascade default skips downstream" do
      assert {:ok, run_id} = ScenarioRunner.start("exhaustion_skip_cascade", "default")

      run = await_terminal_run(run_id)
      assert run.status == "completed"

      {:ok, states} = Runs.list_step_states(Repo, run_id)
      assert step_status(states, "fail_soft") == "skipped"
      assert step_status(states, "downstream") == "skipped"
    end

    test "timeout fast_path completes without timing out" do
      assert {:ok, run_id} = ScenarioRunner.start("timeout", "fast_path")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["slow", "ok"]) == true
    end

    test "delayed one_second_delay completes delayed step" do
      assert {:ok, run_id} = ScenarioRunner.start("delayed", "one_second_delay")

      run = await_terminal_run(run_id, timeout_ms: 20_000)
      assert run.status == "completed"
      assert get_in(run.output, ["delayed", "delayed"]) == true
      assert get_in(run.output, ["delayed", "label"]) == "delayed"
    end

    test "queue_identity mixed_case persists canonical slug output" do
      assert {:ok, run_id} = ScenarioRunner.start("queue_identity", "mixed_case")

      run = await_terminal_run(run_id)
      assert run.status == "completed"
      assert get_in(run.output, ["work", "slug"]) == "MixedCaseDemo"
      assert get_in(run.output, ["work", "value"]) == 42
    end
  end

  describe "application restart acceptance" do
    setup do
      previous = Application.get_env(:pgflow_demo, :scenario_controls_enabled, false)
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)
      on_exit(fn -> Application.put_env(:pgflow_demo, :scenario_controls_enabled, previous) end)
      :ok
    end

    test "completes queued work after worker restart without duplicate cron" do
      cron_before = pgflow_cron_snapshot()

      assert {:ok, run_id} = ScenarioRunner.start("record_job", "delayed_enqueue")
      assert :ok = ScenarioControls.drain_worker("record_job", run_id)

      {:ok, _pid} =
        PgFlow.start_worker(PgflowDemo.Jobs.RecordJob, repo: PgflowDemo.Repo)

      run = await_terminal_run(run_id, timeout_ms: 20_000)
      assert run.status == "completed"
      assert pgflow_cron_snapshot() == cron_before
    end
  end

  describe "ScenarioControls" do
    setup do
      previous = Application.get_env(:pgflow_demo, :scenario_controls_enabled, false)
      on_exit(fn -> Application.put_env(:pgflow_demo, :scenario_controls_enabled, previous) end)
      :ok
    end

    test "rejects controls when disabled" do
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, false)
      run_id = Ecto.UUID.generate()

      assert {:error, :controls_disabled} =
               ScenarioControls.release_handler("recovery", run_id)

      assert {:error, :controls_disabled} =
               ScenarioControls.drain_worker("recovery", run_id)
    end

    test "rejects foreign runs when enabled" do
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)
      assert {:ok, foreign_run_id} = ScenarioRunner.start("retry", "succeed_on_third_attempt")

      assert {:error, :foreign_run} =
               ScenarioControls.release_handler("recovery", foreign_run_id)

      assert {:error, :foreign_run} =
               ScenarioControls.drain_worker("recovery", foreign_run_id)
    end

    test "releases a blocked recovery handler when enabled" do
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)

      assert {:ok, run_id} =
               ScenarioRunner.start("recovery", "blocked_handler")

      :ok = Phoenix.PubSub.subscribe(PgflowDemo.PubSub, "scenario:recovery:#{run_id}")
      # Registration may win the race with subscription; inspect the authoritative
      # registry first, then await its event when the handler has not registered.
      registry = PgflowDemo.ScenarioControls.ReleaseRegistry

      unless Map.has_key?(:sys.get_state(registry), run_id) do
        assert_receive {:handler_started, _handler_pid}, 20_000
      end

      assert {:ok, :released} = ScenarioControls.release_handler("recovery", run_id)

      run = await_terminal_run(run_id, timeout_ms: 20_000)
      assert run.status == "completed"
      assert get_in(run.output, ["wait", "released"]) == true
    end

    test "release registry ignores unexpected messages" do
      registry = PgflowDemo.ScenarioControls.ReleaseRegistry
      pid = Process.whereis(registry)
      ref = Process.monitor(pid)
      send(pid, {:unexpected, :message})
      assert is_map(:sys.get_state(pid))
      refute_received {:DOWN, ^ref, :process, ^pid, _}
      Process.demonitor(ref, [:flush])
    end

    test "drains a demo worker when enabled" do
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)
      assert {:ok, run_id} = ScenarioRunner.start("retry", "succeed_on_third_attempt")
      _run = await_terminal_run(run_id)

      assert :ok = ScenarioControls.drain_worker("retry", run_id)

      assert {:ok, _pid} =
               PgFlow.start_worker(PgflowDemo.Flows.RetryFlow, repo: PgflowDemo.Repo)
    end
  end

  defp step_output(states, slug) do
    states
    |> Enum.find(&(&1.step_slug == slug))
    |> Map.fetch!(:output)
  end

  defp step_status(states, slug) do
    states
    |> Enum.find(&(&1.step_slug == slug))
    |> Map.fetch!(:status)
  end

  defp cron_job_count(job_name) do
    %{rows: [[count]]} =
      Repo.query!("SELECT count(*) FROM cron.job WHERE jobname = $1", [job_name])

    count
  end

  defp pgflow_cron_snapshot do
    %{rows: rows} =
      Repo.query!(
        "SELECT jobname, schedule, command, active FROM cron.job WHERE jobname LIKE 'pgflow:%' ORDER BY jobname"
      )

    rows
  end
end
