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

  describe "cast_flow_slug/1" do
    test "normalizes atom and string slugs and rejects other values" do
      assert {:ok, "my_flow"} = Helpers.cast_flow_slug(:my_flow)
      assert {:ok, "my_flow"} = Helpers.cast_flow_slug("my_flow")
      assert {:error, :invalid_flow_slug} = Helpers.cast_flow_slug(123)
    end

    test "rejects nil, true, and false instead of coercing them via is_atom/1" do
      assert {:error, :invalid_flow_slug} = Helpers.cast_flow_slug(nil)
      assert {:error, :invalid_flow_slug} = Helpers.cast_flow_slug(true)
      assert {:error, :invalid_flow_slug} = Helpers.cast_flow_slug(false)
    end
  end
end
