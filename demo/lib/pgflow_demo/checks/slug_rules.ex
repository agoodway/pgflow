defmodule PgflowDemo.Checks.SlugRules do
  @moduledoc """
  Compares the Elixir slug rules in `PgFlow.Slug` with the database.

  For one candidate slug the check reports:

    * the first broken `PgFlow.Slug.validate/1` rule (or `"ok"`)
    * `pgflow.is_valid_slug($1)` from the installed schema
    * the flow-mode queue name (`lower(slug)`) and whether it fits the
      47-character PGMQ cap, according to both `PgFlow.Slug.validate_queue_name/1`
      and `pgmq.validate_queue_name($1)`

  A slug can be a valid step slug yet an invalid flow slug when its queue
  name is too long. `elixir_db_agree` is true when both sides reach the same
  verdict on the slug rules and on the queue name.
  """

  @behaviour PgflowDemo.ScenarioChecks

  alias Ecto.Adapters.SQL
  alias PgFlow.Slug
  alias PgflowDemo.Repo

  @impl true
  @spec run(map()) :: map()
  def run(%{"slug" => slug}) when is_binary(slug) do
    elixir_rule = elixir_rule(slug)
    queue_name = Slug.queue_name(slug)
    elixir_queue = elixir_queue_check(queue_name)
    db_valid? = db_valid_slug?(slug)
    db_queue = db_queue_check(queue_name)
    step_valid? = elixir_rule == "ok"

    %{
      "slug" => slug,
      "elixir_rule" => elixir_rule,
      "elixir_rule_message" => rule_message(slug),
      "db_is_valid_slug" => db_valid?,
      "flow_queue_name" => queue_name,
      "elixir_queue_check" => elixir_queue,
      "db_queue_check" => db_queue,
      "valid_step_slug" => step_valid?,
      "valid_flow_slug" => step_valid? and elixir_queue == "ok",
      "elixir_db_agree" => step_valid? == db_valid? and elixir_queue == db_queue
    }
  end

  defp elixir_rule(slug) do
    case Slug.validate(slug) do
      :ok -> "ok"
      {:error, reason} -> Atom.to_string(reason)
    end
  end

  defp rule_message(slug) do
    case Slug.validate(slug) do
      :ok -> nil
      {:error, reason} -> Slug.describe(reason)
    end
  end

  defp elixir_queue_check(queue_name) do
    case Slug.validate_queue_name(queue_name) do
      :ok -> "ok"
      {:error, reason} -> Atom.to_string(reason)
    end
  end

  defp db_valid_slug?(slug) do
    %{rows: [[valid?]]} = SQL.query!(Repo, "SELECT pgflow.is_valid_slug($1::text)", [slug])
    valid?
  end

  # pgmq.validate_queue_name raises on rejection; the transaction isolates the
  # error so a surrounding sandbox transaction stays usable.
  defp db_queue_check(queue_name) do
    case Repo.transaction(fn -> validate_queue_name(queue_name) end) do
      {:ok, :ok} -> "ok"
      {:error, error} -> db_queue_rejection(error)
    end
  end

  defp validate_queue_name(queue_name) do
    case SQL.query(Repo, "SELECT pgmq.validate_queue_name($1::text)", [queue_name]) do
      {:ok, _} -> :ok
      {:error, error} -> Repo.rollback(error)
    end
  end

  defp db_queue_rejection(%Postgrex.Error{postgres: %{message: message}}) do
    if message =~ "too long", do: "queue_name_too_long", else: "rejected: #{message}"
  end

  defp db_queue_rejection(error), do: "rejected: #{Exception.message(error)}"
end
