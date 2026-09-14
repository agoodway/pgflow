defmodule PgflowDemo.Flows.ParallelFlow do
  @moduledoc """
  Deterministic parallel branches with fan-in aggregation.
  """

  use PgFlow.Flow

  @flow slug: :parallel_flow, max_attempts: 1, timeout: 30

  step :start do
    fn input, _ctx ->
      %{"seed" => input["seed"] || 1}
    end
  end

  step :left, depends_on: [:start] do
    fn deps, _ctx ->
      %{"value" => deps["start"]["seed"] * 2}
    end
  end

  step :right, depends_on: [:start] do
    fn deps, _ctx ->
      %{"value" => deps["start"]["seed"] * 3}
    end
  end

  step :merge, depends_on: [:left, :right] do
    fn deps, _ctx ->
      %{"sum" => deps["left"]["value"] + deps["right"]["value"]}
    end
  end
end
