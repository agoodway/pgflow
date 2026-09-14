defmodule PgflowDemo.Flows.Policies.WhenUnmetFailFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :policy_when_unmet_fail_flow, max_attempts: 1, timeout: 30

  step :seed do
    fn input, _ctx -> %{"enabled" => input["enabled"] || false} end
  end

  step :gated,
    depends_on: [:seed],
    if: %{"seed" => %{"enabled" => true}},
    when_unmet: :fail do
    fn _deps, _ctx -> %{"ok" => true} end
  end
end
