defmodule PgflowDemo.VerifyScenarios do
  @moduledoc """
  Runs executable catalogue presets through `PgflowDemo.ScenarioRunner` and
  compares persisted outcomes to catalogue expectations.
  """

  alias PgFlow.Runs
  alias PgflowDemo.{Repo, ScenarioControls, ScenarioRunner, Scenarios}

  @default_timeout_ms 30_000

  @type preset_result :: %{
          scenario_id: String.t(),
          preset: String.t(),
          pass: boolean(),
          expected: map(),
          actual: map(),
          message: String.t() | nil
        }

  @type report :: %{
          pass: boolean(),
          results: [preset_result()],
          walkthrough: [map()],
          llm: map(),
          executed_count: non_neg_integer(),
          run_history_preserved: boolean()
        }

  @doc """
  Verifies every executable preset in the catalogue.
  """
  @spec run(keyword()) :: report()
  def run(opts \\ []) do
    include_llm? = Keyword.get(opts, :include_llm, false)
    llm_configured? = Keyword.get(opts, :llm_configured?, llm_configured?())

    run_count_before = run_count()
    cron_before = pgflow_cron_snapshot()
    controls_before = Application.get_env(:pgflow_demo, :scenario_controls_enabled, false)
    Application.put_env(:pgflow_demo, :scenario_controls_enabled, true)

    try do
      {executable_results, executed_count} =
        Scenarios.list()
        |> Enum.filter(&(&1.kind == :executable))
        |> Enum.flat_map(fn descriptor ->
          Enum.map(descriptor.presets, fn {preset_key, preset} ->
            verify_one(descriptor.id, preset_key, expected: preset.expected)
          end)
        end)
        |> then(fn results -> {results, length(results)} end)

      walkthrough =
        Scenarios.list()
        |> Enum.filter(&(&1.kind == :walkthrough))
        |> Enum.map(fn descriptor ->
          %{
            scenario_id: descriptor.id,
            title: descriptor.title,
            walkthrough: descriptor.walkthrough
          }
        end)

      llm = llm_report(include_llm?, llm_configured?)
      cron_after = pgflow_cron_snapshot()
      restore_cron!(cron_before, cron_after)

      pass =
        Enum.all?(executable_results, & &1.pass) and
          run_count() >= run_count_before + executed_count and
          cron_after == cron_before

      %{
        pass: pass,
        results: executable_results,
        walkthrough: walkthrough,
        llm: llm,
        executed_count: executed_count,
        run_history_preserved: run_count() >= run_count_before
      }
    after
      Application.put_env(:pgflow_demo, :scenario_controls_enabled, controls_before)
    end
  end

  @doc """
  Verifies one executable preset, optionally overriding expected outcomes (for tests).
  """
  @spec verify_one(String.t(), String.t(), keyword()) :: preset_result()
  def verify_one(scenario_id, preset_key, opts \\ []) do
    expected_override = Keyword.get(opts, :expected)
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)

    with {:ok, _descriptor} <- Scenarios.fetch(scenario_id),
         {:ok, preset} <- Scenarios.validate_preset(scenario_id, preset_key),
         expected <- expected_override || preset.expected,
         {:ok, run_id} <- start_preset(scenario_id, preset_key),
         :ok <- maybe_release_recovery(scenario_id, run_id),
         {:ok, loaded} <- await_and_load(run_id, timeout_ms),
         actual <- actual_outcome(loaded, expected),
         :ok <- compare(expected, actual) do
      result(scenario_id, preset_key, true, expected, actual, nil)
    else
      {:error, :mismatch, expected, actual, message} ->
        result(scenario_id, preset_key, false, expected, actual, message)

      {:error, reason} ->
        expected =
          case Scenarios.validate_preset(scenario_id, preset_key) do
            {:ok, preset} -> expected_override || preset.expected
            _ -> expected_override || %{}
          end

        result(scenario_id, preset_key, false, expected, %{}, inspect(reason))
    end
  end

  @doc """
  Prints a human-readable verification report.
  """
  @spec print_report(report()) :: :ok
  def print_report(%{pass: pass} = report) do
    IO.puts("")
    IO.puts("PgFlow demo scenario verification")
    IO.puts(String.duplicate("=", 60))

    for result <- report.results do
      status = if result.pass, do: "PASS", else: "FAIL"

      IO.puts(
        "  [#{status}] #{result.scenario_id}/#{result.preset} expected=#{inspect(result.expected)} actual=#{inspect(result.actual)}"
      )

      if result.message do
        IO.puts("         #{result.message}")
      end
    end

    IO.puts("")
    IO.puts("Walkthrough-only scenarios (not required for baseline gate):")

    for entry <- report.walkthrough do
      IO.puts("  - #{entry.scenario_id}: #{entry.walkthrough}")
    end

    IO.puts("")
    IO.puts("LLM integration: #{report.llm.status} — #{report.llm.reason}")
    IO.puts("Run history preserved: #{report.run_history_preserved}")
    IO.puts("Executed presets: #{report.executed_count}")
    IO.puts(String.duplicate("=", 60))

    if pass do
      IO.puts("ALL EXECUTABLE SCENARIOS PASSED")
    else
      IO.puts("SCENARIO VERIFICATION FAILED")
    end

    :ok
  end

  defp start_preset(scenario_id, preset_key) do
    ScenarioRunner.start(scenario_id, preset_key)
  end

  defp maybe_release_recovery("recovery", run_id) do
    release_recovery_handler(run_id)
  end

  defp maybe_release_recovery(_scenario_id, _run_id), do: :ok

  defp release_recovery_handler(run_id) do
    deadline = System.monotonic_time(:millisecond) + 20_000
    release_recovery_handler_loop(run_id, deadline)
  end

  defp release_recovery_handler_loop(run_id, deadline) do
    case ScenarioControls.release_handler("recovery", run_id) do
      {:ok, :released} ->
        :ok

      {:error, :not_found} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :recovery_release_timeout}
        else
          Process.sleep(50)
          release_recovery_handler_loop(run_id, deadline)
        end

      other ->
        other
    end
  end

  defp await_and_load(run_id, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    with :ok <- await_terminal(run_id, deadline) do
      ScenarioRunner.load(run_id)
    end
  end

  defp await_terminal(run_id, deadline) do
    case Runs.get(Repo, run_id) do
      {:ok, %{status: status}} when status in ["completed", "failed"] ->
        :ok

      {:ok, _} ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(100)
          await_terminal(run_id, deadline)
        end

      error ->
        error
    end
  end

  defp actual_outcome(%{status: status, tasks: tasks, step_states: states}, expected) do
    base = %{
      "run_status" => status,
      "step_statuses" => Map.new(states, &{&1.step_slug, &1.status}),
      "outputs" =>
        states
        |> Map.new(&{&1.step_slug, &1.output})
        |> Map.take(Map.keys(Map.get(expected, "outputs", %{})))
    }

    if Map.has_key?(expected, "attempts_count") do
      attempts =
        tasks
        |> Enum.find(&(&1.step_slug == "retry_step"))
        |> case do
          nil -> nil
          task -> task.attempts_count
        end

      Map.put(base, "attempts_count", attempts)
    else
      base
    end
  end

  defp compare(expected, actual) do
    mismatches =
      Enum.filter(expected, fn {key, value} -> Map.get(actual, key) != value end)

    case mismatches do
      [] ->
        :ok

      [{key, expected_value} | _] ->
        actual_value = Map.get(actual, key)

        {:error, :mismatch, expected, actual,
         "#{key}: expected #{inspect(expected_value)}, got #{inspect(actual_value)}"}
    end
  end

  defp result(scenario_id, preset, pass, expected, actual, message) do
    %{
      scenario_id: scenario_id,
      preset: preset,
      pass: pass,
      expected: expected,
      actual: actual,
      message: message
    }
  end

  defp llm_report(false, _configured?) do
    %{status: :skipped, reason: "opt-in only; pass --include-llm to report separately"}
  end

  defp llm_report(true, false) do
    %{status: :skipped, reason: "article scenario requires AI_API_KEY; not configured"}
  end

  defp llm_report(true, true) do
    %{status: :opt_in, reason: "article scenario configured; run FlowDemoLive manually"}
  end

  defp llm_configured? do
    case System.get_env("AI_API_KEY") do
      nil -> false
      "" -> false
      _ -> true
    end
  end

  defp run_count do
    %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM pgflow.runs")
    count
  end

  defp pgflow_cron_snapshot do
    %{rows: rows} =
      Repo.query!(
        "SELECT jobname, schedule, command, active FROM cron.job WHERE jobname LIKE 'pgflow:%' ORDER BY jobname"
      )

    rows
  end

  defp restore_cron!(before, after_rows) when before == after_rows, do: :ok

  defp restore_cron!(before, _after_rows) do
    before_names = MapSet.new(before, &List.first/1)

    %{rows: current} =
      Repo.query!("SELECT jobname FROM cron.job WHERE jobname LIKE 'pgflow:%' ORDER BY jobname")

    for [jobname | _] <- current, not MapSet.member?(before_names, jobname) do
      Repo.query!("SELECT cron.unschedule($1::text)", [jobname])
    end

    :ok
  end
end
