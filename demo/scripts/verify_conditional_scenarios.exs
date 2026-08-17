# demo/scripts/verify_conditional_scenarios.exs
#
# Verifies the three conditional-step scenarios showcased by
# `PgflowDemo.Flows.OnboardingFlow` (demo/lib/pgflow_demo/flows/onboarding_flow.ex):
#
#   1. plan: "premium", fail_email: false
#      -> every step runs and completes (the `if:` on setup_premium is met).
#   2. plan: "free", fail_email: false
#      -> setup_premium's `if:` is unmet, so it is skipped
#         (when_unmet: :skip_cascade, skip_reason "condition_unmet"), and its
#         dependent activate_perk cascades to skipped/"dependency_skipped".
#         The rest of the run still completes.
#   3. plan: "premium", fail_email: true
#      -> send_welcome's handler raises, max_attempts: 1 exhausts immediately,
#         and when_exhausted: :skip marks it skipped/"handler_failed" instead
#         of failing the run. Every other step still completes.
#
# This starts each scenario as a REAL flow run against the configured repo
# (dev by default) and waits for it to reach a terminal status via
# `PgFlow.Client.start_flow_sync/3`. The app's supervisor starts pgflow's
# workers on boot, so `mix run` actually executes the flow end to end -
# nothing here is mocked.
#
# Requirements:
#   - Dev database created and migrated: `mix ecto.create && mix ecto.migrate`
#     (from demo/), which includes the `compile_onboarding_flow` migration.
#   - Run from the demo/ directory:
#       mix run scripts/verify_conditional_scenarios.exs
#
# Exits non-zero (via System.halt/1) if any scenario's step states don't
# match the expected outcomes above.

alias PgFlow.Client

defmodule VerifyConditionalScenarios do
  @flow :onboarding_flow
  @timeout 30_000

  # Each expected step state is {step_slug, status, skip_reason}, in the
  # order they should print. Derived directly from onboarding_flow.ex.
  @scenarios [
    %{
      name: "premium plan, email ok",
      input: %{"plan" => "premium", "fail_email" => false},
      expected: [
        {"create_account", "completed", nil},
        {"setup_premium", "completed", nil},
        {"activate_perk", "completed", nil},
        {"send_welcome", "completed", nil},
        {"finish", "completed", nil}
      ]
    },
    %{
      name: "free plan, email ok (condition unmet -> skip cascade)",
      input: %{"plan" => "free", "fail_email" => false},
      expected: [
        {"create_account", "completed", nil},
        {"setup_premium", "skipped", "condition_unmet"},
        {"activate_perk", "skipped", "dependency_skipped"},
        {"send_welcome", "completed", nil},
        {"finish", "completed", nil}
      ]
    },
    %{
      name: "premium plan, email fails (handler exhausted -> skip)",
      input: %{"plan" => "premium", "fail_email" => true},
      expected: [
        {"create_account", "completed", nil},
        {"setup_premium", "completed", nil},
        {"activate_perk", "completed", nil},
        {"send_welcome", "skipped", "handler_failed"},
        {"finish", "completed", nil}
      ]
    }
  ]

  def run do
    results = Enum.map(@scenarios, &run_scenario/1)

    IO.puts("")
    IO.puts(String.duplicate("=", 60))

    if Enum.all?(results) do
      IO.puts("ALL SCENARIOS PASSED")
    else
      IO.puts("SOME SCENARIOS FAILED")
      System.halt(1)
    end
  end

  defp run_scenario(%{name: name, input: input, expected: expected}) do
    IO.puts("")
    IO.puts("Scenario: #{name}")
    IO.puts("Input: #{inspect(input)}")

    case Client.start_flow_sync(@flow, input, timeout: @timeout) do
      {:ok, run} ->
        check(run, expected)

      {:error, %PgFlow.Schema.Run{} = run} ->
        check(run, expected)

      {:error, :timeout} ->
        IO.puts("  FAIL: run did not reach a terminal status within #{@timeout}ms")
        false

      {:error, reason} ->
        IO.puts("  FAIL: could not start flow: #{inspect(reason)}")
        false
    end
  end

  defp check(run, expected) do
    {:ok, %{step_states: step_states}} = Client.get_run_with_states(run.run_id)

    actual =
      Map.new(step_states, fn state -> {state.step_slug, {state.status, state.skip_reason}} end)

    IO.puts("  run_id: #{run.run_id}  run status: #{run.status}")
    print_table(expected, actual)

    mismatches =
      Enum.filter(expected, fn {slug, status, skip_reason} ->
        Map.get(actual, slug) != {status, skip_reason}
      end)

    if mismatches == [] do
      IO.puts("  PASS")
      true
    else
      IO.puts("  FAIL: unexpected state for #{inspect(Enum.map(mismatches, &elem(&1, 0)))}")
      false
    end
  end

  defp print_table(expected, actual) do
    header =
      String.pad_trailing("step", 18) <>
        String.pad_trailing("status", 12) <> "skip_reason"

    IO.puts("  " <> header)
    IO.puts("  " <> String.duplicate("-", String.length(header)))

    Enum.each(expected, fn {slug, _status, _skip_reason} ->
      {status, skip_reason} = Map.get(actual, slug, {"(missing)", nil})

      IO.puts(
        "  " <>
          String.pad_trailing(slug, 18) <>
          String.pad_trailing(status, 12) <>
          (skip_reason || "-")
      )
    end)
  end
end

VerifyConditionalScenarios.run()
