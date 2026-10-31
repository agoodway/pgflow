defmodule PgFlow.SlugTest do
  use ExUnit.Case, async: true

  alias PgFlow.Slug

  @cases [
    {"process_order", :ok},
    {"Step1", :ok},
    {"a", :ok},
    {"", {:error, :empty}},
    {String.duplicate("a", 129), {:error, :too_long}},
    {"has-dash", {:error, :bad_chars}},
    {"1abc", {:error, :bad_chars}},
    {"run", {:error, :reserved}},
    {"_private", {:error, :bad_start}},
    {"trailing_", {:error, :bad_end}},
    {"a__b", {:error, :double_underscore}},
    {nil, {:error, :empty}},
    {String.duplicate("a", 128), :ok}
  ]

  for {slug, expected} <- @cases do
    test "validate(#{inspect(slug)}) == #{inspect(expected)}" do
      assert Slug.validate(unquote(slug)) == unquote(Macro.escape(expected))
    end
  end

  test "Run is not reserved (upstream comparison is case-sensitive)" do
    assert Slug.validate("Run") == :ok
  end

  test "atoms are validated by their string form" do
    assert Slug.valid?(:process_order)
    refute Slug.valid?(:_x)
  end

  test "queue_name/1 lowercases and validate_queue_name/1 enforces 47 chars" do
    assert Slug.queue_name("OrderFlow") == "orderflow"
    assert Slug.validate_queue_name(String.duplicate("a", 47)) == :ok
    assert Slug.validate_queue_name(String.duplicate("a", 48)) == {:error, :queue_name_too_long}
  end

  test "describe/1 names the rule" do
    assert Slug.describe(:double_underscore) =~ "__"
  end
end
