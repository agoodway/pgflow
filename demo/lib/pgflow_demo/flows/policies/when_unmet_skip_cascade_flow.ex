defmodule PgflowDemo.Flows.Policies.WhenUnmetSkipCascadeFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :policy_when_unmet_skip_cascade_flow, max_attempts: 1, timeout: 30

  step :seed do
    fn input, _ctx -> %{"enabled" => input["enabled"] || false} end
  end

  step :gated,
    depends_on: [:seed],
    if: %{"seed" => %{"enabled" => true}},
    when_unmet: :skip_cascade do
    fn _deps, _ctx -> %{"ok" => true} end
  end

  step :downstream, depends_on: [:gated] do
    fn _deps, _ctx -> %{"downstream" => true} end
  end
end
