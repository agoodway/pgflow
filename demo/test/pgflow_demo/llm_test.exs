defmodule PgflowDemo.LLMTest do
  use ExUnit.Case, async: true

  alias PgflowDemo.LLM

  test "extract_keyword_values preserves string then atom fallback precedence" do
    assert LLM.extract_keyword_values(%{"keywords" => ["string"], keywords: ["atom"]}) == [
             "string"
           ]

    assert LLM.extract_keyword_values(%{"keywords" => nil, keywords: ["atom"]}) == ["atom"]
    assert LLM.extract_keyword_values(%{"keywords" => nil, keywords: nil}) == []
  end
end
