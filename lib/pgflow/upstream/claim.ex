defmodule PgFlow.Upstream.Claim do
  @moduledoc """
  Recorded eight-column `start_tasks/4` body from core V02.

  Helpers V05 installs the recorded definition with schema placeholders
  expanded. The checksum guards this fragment; the importer determinism check
  separately verifies its relationship to core V02.
  """

  @fragment_path "priv/pgflow_helpers/sql/fragments/start_tasks_eight_column_v02.sql"
  @checksum "41dcf9970da6d194fdd66606b11f516d40a2edeb2fec074eeb47ae8259f9ca3c"

  @doc "Returns the recorded SQL fragment with `$SCHEMA$` placeholders."
  @spec eight_column_start_tasks_sql() :: String.t()
  def eight_column_start_tasks_sql do
    :pgflow
    |> Application.app_dir(@fragment_path)
    |> File.read!()
  end

  @doc "SHA-256 of the recorded fragment bytes."
  @spec checksum() :: String.t()
  def checksum, do: @checksum

  @doc "Verifies the on-disk fragment still matches the recorded checksum."
  @spec verify!() :: :ok
  def verify! do
    actual =
      eight_column_start_tasks_sql()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    if actual == @checksum do
      :ok
    else
      raise "recorded start_tasks fragment checksum mismatch: expected #{@checksum}, got #{actual}"
    end
  end
end
