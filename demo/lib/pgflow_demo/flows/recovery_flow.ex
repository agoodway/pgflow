defmodule PgflowDemo.Flows.RecoveryFlow do
  @moduledoc """
  Controlled handler for recovery and drain walkthroughs.
  """

  use PgFlow.Flow

  alias PgflowDemo.ScenarioControls.ReleaseRegistry

  @flow slug: :recovery_flow, max_attempts: 1, timeout: 60

  step :wait do
    fn _input, ctx ->
      :ok = ReleaseRegistry.register(ctx.run_id, self())

      receive do
        :release -> %{"released" => true}
      end
    end
  end
end
