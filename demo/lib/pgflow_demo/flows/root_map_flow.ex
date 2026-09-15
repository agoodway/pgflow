defmodule PgflowDemo.Flows.RootMapFlow do
  @moduledoc """
  Root map and map-to-map propagation with empty and scalar inputs.
  """

  use PgFlow.Flow

  @flow slug: :root_map_flow, max_attempts: 1, timeout: 30

  step :items do
    fn input, _ctx ->
      case Map.get(input, "items") do
        list when is_list(list) -> list
        nil -> []
        scalar -> [scalar]
      end
    end
  end

  map :root_items, array: :items do
    fn input, _ctx ->
      %{"item" => input}
    end
  end

  map :child_items, array: :root_items do
    fn input, _ctx ->
      %{"child" => input["item"]}
    end
  end

  step :summarize, depends_on: [:child_items] do
    fn deps, _ctx ->
      children = deps["child_items"] || []
      %{"count" => length(children), "items" => Enum.map(children, & &1["child"])}
    end
  end
end
