defmodule PgFlow.Type.JSONTest do
  use ExUnit.Case, async: true

  alias PgFlow.Type.JSON

  describe "cast/1, load/1, and dump/1" do
    for value <- [%{"key" => "value"}, ["one", 2], "value", 42, true, false, nil] do
      test "round-trips #{inspect(value)}" do
        value = unquote(Macro.escape(value))

        assert {:ok, ^value} = JSON.cast(value)
        assert {:ok, ^value} = JSON.load(value)
        assert {:ok, ^value} = JSON.dump(value)
      end
    end

    test "rejects non-canonical JSON values instead of changing them on round-trip" do
      invalid_utf8 = <<255>>

      for value <- [
            {:not, :json},
            :atom,
            %{atom_key: "value"},
            ["value", :atom],
            [1 | 2],
            invalid_utf8,
            %{invalid_utf8 => "value"}
          ] do
        assert :error = JSON.cast(value)
        assert :error = JSON.load(value)
        assert :error = JSON.dump(value)
        assert :error = Ecto.Type.adapter_dump(Ecto.Adapters.Postgres, JSON, value)
        assert :error = Ecto.Type.adapter_load(Ecto.Adapters.Postgres, JSON, value)
      end
    end
  end

  test "passes list and scalar values through the PostgreSQL adapter" do
    for value <- [["one", 2], "value"] do
      assert {:ok, ^value} = Ecto.Type.adapter_dump(Ecto.Adapters.Postgres, JSON, value)
      assert {:ok, ^value} = Ecto.Type.adapter_load(Ecto.Adapters.Postgres, JSON, value)
    end
  end
end
