defmodule PgFlow.VersionTest do
  use ExUnit.Case, async: true

  test "worker_stamp/0 identifies the Elixir runtime" do
    assert PgFlow.Version.worker_stamp() ==
             "elixir-" <> to_string(Application.spec(:pgflow, :vsn))
  end
end
