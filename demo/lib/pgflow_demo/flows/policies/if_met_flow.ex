defmodule PgflowDemo.Flows.Policies.IfMetFlow do
  @moduledoc false
  use PgFlow.Flow

  @flow slug: :policy_if_met_flow, max_attempts: 1, timeout: 30

  step :seed do
    fn input, _ctx -> %{"mode" => input["mode"] || "active"} end
  end

  step :conditional,
    depends_on: [:seed],
    if: %{"seed" => %{"mode" => "active"}},
    when_unmet: :skip do
    fn deps, _ctx -> %{"ran" => true, "mode" => deps["seed"]["mode"]} end
  end
end
