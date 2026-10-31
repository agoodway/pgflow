defmodule PgFlow.Test.GatedFlow do
  @moduledoc """
  A one-step flow whose handler reports its start to the test process and then
  blocks until it receives `:release`, so tests can hold tasks active.
  """
  use PgFlow.Flow

  @flow slug: :gated_flow, max_attempts: 1

  step :gate do
    fn input, _ctx ->
      test_pid = :persistent_term.get({__MODULE__, :test_pid})
      send(test_pid, {:task_started, self(), input["n"]})

      receive do
        :release -> :ok
      end

      %{n: input["n"]}
    end
  end
end
