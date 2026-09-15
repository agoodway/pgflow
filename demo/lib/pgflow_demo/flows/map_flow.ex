defmodule PgflowDemo.Flows.MapFlow do
  @moduledoc """
  Dependent map over a generated array with fan-in aggregation.
  """

  use PgFlow.Flow

  @flow slug: :map_flow, max_attempts: 1, timeout: 30

  step :generate do
    fn input, _ctx ->
      count = Map.get(input, "count", 3)
      count = if is_integer(count), do: count |> max(0) |> min(10), else: 0
      if count == 0, do: [], else: Enum.to_list(1..count)
    end
  end

  map :process_items, array: :generate do
    fn item, _ctx ->
      %{"doubled" => item * 2}
    end
  end

  step :aggregate, depends_on: [:process_items] do
    fn deps, _ctx ->
      items = deps["process_items"]
      %{"total" => Enum.reduce(items, 0, fn item, acc -> acc + item["doubled"] end)}
    end
  end
end
