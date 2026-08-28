defmodule PgFlowDashboard.Components.RunHistoryGridTest do
  use ExUnit.Case, async: true

  alias PgFlow.RunHistoryCell
  alias PgFlowDashboard.Components.RunHistoryGrid

  test "groups typed history cells by step for the activity grid" do
    started_at = ~U[2026-08-28 12:00:00.000000Z]

    cells = [
      RunHistoryCell.new(%{
        run_id: "f53bea39-3df7-4020-9047-80ddd19109d0",
        started_at: started_at,
        step_slug: "charge",
        status: "completed",
        duration_ms: Decimal.new(42)
      }),
      RunHistoryCell.new(%{
        run_id: "f53bea39-3df7-4020-9047-80ddd19109d0",
        started_at: started_at,
        step_slug: "receipt",
        status: "started",
        duration_ms: nil
      })
    ]

    assert %{"charge" => [first], "receipt" => [second]} = RunHistoryGrid.group_cells(cells)
    assert %RunHistoryCell{step_slug: "charge"} = first
    assert %RunHistoryCell{step_slug: "receipt"} = second
  end
end
