defmodule PgFlow.Test.UpstreamUpgrade do
  @moduledoc false
  # Fixture, migration, and catalog helpers shared by the upstream upgrade tests.

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias EctoEvolver.Adapters.Postgres, as: EvolverPostgres
  alias PgFlow.Test.UpstreamFixture
  alias PgFlow.TestRepo

  # Each fixture is a fresh database, so one fixed version per migration is
  # safe. Apply core steps before helpers steps.
  @steps %{
    core_v02: {9_000_000_102, PgFlow.Upstream.CoreUpgradeMigration},
    core_v03: {9_000_000_203, PgFlow.Upstream.CoreV03Migration},
    helpers_v05: {9_000_000_105, PgFlow.Upstream.HelpersUpgradeMigration},
    helpers_v06: {9_000_000_106, PgFlow.Upstream.HelpersV06Migration},
    helpers_v07: {9_000_000_207, PgFlow.Upstream.HelpersV07Migration}
  }

  @type step :: :core_v02 | :core_v03 | :helpers_v05 | :helpers_v06 | :helpers_v07

  @doc """
  Sets up an upstream fixture database (`UpstreamFixture.setup!/1` with
  `opts`) and tears it down when the test exits.
  """
  @spec fixture!(keyword()) :: map()
  def fixture!(opts \\ []) do
    fixture = UpstreamFixture.setup!(opts)
    Process.unlink(fixture.repo_pid)
    on_exit(fn -> UpstreamFixture.teardown!(fixture) end)
    fixture
  end

  @doc "Runs `fun` with `PgFlow.TestRepo` pointed at the fixture database."
  @spec in_fixture(map(), (-> result)) :: result when result: term()
  def in_fixture(fixture, fun) do
    previous = TestRepo.put_dynamic_repo(fixture.repo_name)

    try do
      fun.()
    after
      TestRepo.put_dynamic_repo(previous)
    end
  end

  @doc "Runs one upgrade migration step, or each step of a list in order."
  @spec migrate!(step() | [step()]) :: :ok
  def migrate!(steps) when is_list(steps), do: Enum.each(steps, &migrate!/1)

  def migrate!(step) do
    {version, migration} = Map.fetch!(@steps, step)
    Ecto.Migrator.up(TestRepo, version, migration, log: false)
    :ok
  end

  @doc "Returns the installed pgflow core version."
  @spec core_version() :: non_neg_integer()
  def core_version do
    EvolverPostgres.get_version(TestRepo, "pgflow", {:view, "pgflow_version"})
  end

  @doc "Returns the installed pgflow helpers version."
  @spec helpers_version() :: non_neg_integer()
  def helpers_version do
    EvolverPostgres.get_version(TestRepo, "pgflow", {:view, "extensions_version"})
  end

  @doc "Runs `sql` and returns its single value."
  @spec scalar!(String.t(), list()) :: term()
  def scalar!(sql, params \\ []) do
    %{rows: [[value]]} = TestRepo.query!(sql, params)
    value
  end

  @doc "Returns the identity arguments of each `pgflow` function named `name`, sorted."
  @spec function_arguments(String.t()) :: [String.t()]
  def function_arguments(name) do
    TestRepo.query!(
      """
      SELECT pg_get_function_identity_arguments(p.oid)
      FROM pg_proc AS p
      JOIN pg_namespace AS n ON n.oid = p.pronamespace
      WHERE n.nspname = 'pgflow' AND p.proname = $1
      ORDER BY 1
      """,
      [name]
    ).rows
    |> List.flatten()
  end

  @doc "Reports whether `pgflow.<table>` has `column`."
  @spec column_exists?(String.t(), String.t()) :: boolean()
  def column_exists?(table, column) do
    scalar!(
      """
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'pgflow' AND table_name = $1 AND column_name = $2
      )
      """,
      [table, column]
    )
  end

  @doc "Returns the field names of `pgflow.step_task_record` in order."
  @spec record_fields() :: [String.t()]
  def record_fields do
    TestRepo.query!("""
    SELECT attname
    FROM pg_attribute
    WHERE attrelid = (
      SELECT typrelid FROM pg_type WHERE oid = 'pgflow.step_task_record'::regtype
    )
      AND attnum > 0
      AND NOT attisdropped
    ORDER BY attnum
    """).rows
    |> List.flatten()
  end
end
