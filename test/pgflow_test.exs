defmodule PgFlowTest do
  use ExUnit.Case, async: true

  describe "child_spec/1" do
    test "returns valid child spec" do
      spec = PgFlow.child_spec(repo: FakeRepo)

      assert spec.id == PgFlow
      assert spec.type == :supervisor
      assert {PgFlow, :start_link, [[repo: FakeRepo]]} = spec.start
    end
  end
end
