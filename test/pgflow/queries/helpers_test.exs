defmodule PgFlow.Queries.HelpersTest do
  use ExUnit.Case, async: true

  alias PgFlow.Queries.Helpers

  describe "cast_uuid/1 and optional_uuid/1" do
    test "cast valid UUIDs and reject invalid values" do
      uuid = Ecto.UUID.generate()

      assert {:ok, ^uuid} = Helpers.cast_uuid(uuid)
      assert {:error, :invalid_id} = Helpers.cast_uuid("not-a-uuid")
      assert {:ok, nil} = Helpers.optional_uuid(nil)
      assert {:ok, ^uuid} = Helpers.optional_uuid(uuid)
    end
  end

  describe "positive_limit/2" do
    test "accepts positive limits and falls back to the supplied default" do
      assert Helpers.positive_limit([limit: 25], 50) == 25
      assert Helpers.positive_limit([limit: 0], 50) == 50
      assert Helpers.positive_limit([limit: -1], 50) == 50
      assert Helpers.positive_limit([limit: "25"], 50) == 50
      assert Helpers.positive_limit([], 50) == 50
    end
  end
end
