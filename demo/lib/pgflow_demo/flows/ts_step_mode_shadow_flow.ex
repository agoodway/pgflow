defmodule PgflowDemo.Flows.TsStepModeShadowFlow do
  @moduledoc """
  Elixir definition that shares its slug with a TypeScript-owned step-mode flow.

  The `step_mode_guard` check provisions `ts_step_mode_shadow` in the database
  with `queue_mode = 'step'` and then runs worker bootstrap for this module.
  It is never listed in the `PgFlow.Supervisor` flows, and its handlers never
  run: Elixir workers refuse step-mode flows before compilation.
  """

  use PgFlow.Flow

  @flow slug: :ts_step_mode_shadow, max_attempts: 1, timeout: 30

  step :fetch_order do
    fn _input, _ctx -> raise "ts_step_mode_shadow is owned by a TypeScript step-mode worker" end
  end

  step :notify, depends_on: [:fetch_order] do
    fn _deps, _ctx -> raise "ts_step_mode_shadow is owned by a TypeScript step-mode worker" end
  end
end
