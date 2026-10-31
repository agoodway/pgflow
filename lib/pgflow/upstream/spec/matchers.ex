defmodule PgFlow.Upstream.Spec.Matchers do
  @moduledoc false
  # Statement matchers and rewrites shared by the versioned upstream specs.

  @doc false
  @spec set_lock_timeout?(String.t()) :: boolean()
  def set_lock_timeout?(statement) do
    Regex.match?(~r/^SET lock_timeout = '10s'$/m, statement)
  end

  @doc false
  @spec localize_lock_timeout(String.t()) :: {:ok, String.t()}
  def localize_lock_timeout(statement) do
    {:ok, Regex.replace(~r/^SET lock_timeout/m, statement, "SET LOCAL lock_timeout")}
  end

  @doc false
  @spec start_tasks_definition?(String.t()) :: boolean()
  def start_tasks_definition?(statement) do
    Regex.match?(~r/CREATE (?:OR REPLACE )?FUNCTION "pgflow"\."start_tasks" /s, statement)
  end
end
