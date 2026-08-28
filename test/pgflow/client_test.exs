defmodule PgFlow.ClientTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Client
  alias PgFlow.TestRepo

  @moduletag timeout: 30_000
  @moduletag :integration

  defmodule ClientTestFlow do
    use PgFlow.Flow

    @flow slug: :client_test_flow, max_attempts: 3

    step :process do
      fn input, _ctx ->
        %{result: input["value"]}
      end
    end
  end

  setup do
    Sandbox.mode(TestRepo, :auto)
    TestRepo.query!("SELECT pgflow_tests.reset_db()")

    # Set repo in persistent_term for Client to find
    :persistent_term.put({PgFlow, :repo}, TestRepo)

    on_exit(fn ->
      :persistent_term.erase({PgFlow, :repo})
      Sandbox.mode(TestRepo, :manual)
    end)

    # Compile the flow in the DB
    flow_slug = compile_flow(ClientTestFlow)
    {:ok, flow_slug: flow_slug}
  end

  defp compile_flow(flow_module) do
    definition = flow_module.__pgflow_definition__()
    flow_slug = Atom.to_string(definition.slug)

    max_attempts = definition.opts[:max_attempts] || 3
    base_delay = definition.opts[:base_delay] || 1
    timeout = definition.opts[:timeout] || 30

    TestRepo.query!(
      "SELECT pgflow.create_flow($1, $2, $3, $4)",
      [flow_slug, max_attempts, base_delay, timeout]
    )

    for step <- definition.steps do
      step_slug = Atom.to_string(step.slug)
      deps = Enum.map(step.depends_on, &Atom.to_string/1)
      step_type = Atom.to_string(step.step_type)

      TestRepo.query!(
        "SELECT pgflow.add_step($1, $2, $3::text[], $4, $5, $6, $7, $8)",
        [
          flow_slug,
          step_slug,
          deps,
          step.max_attempts,
          step.base_delay,
          step.timeout,
          step.start_delay,
          step_type
        ]
      )
    end

    flow_slug
  end

  # ── start_flow ─────────────────────────────────────────────────────

  describe "start_flow/2" do
    test "starts flow with module name", %{flow_slug: _flow_slug} do
      {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 42})
      assert is_binary(run_id)
      assert {:ok, _} = Ecto.UUID.cast(run_id)
    end

    test "resolves slug when flow module is not yet loaded" do
      # Regression: boot-race where the flow module exists on disk but has not
      # been loaded into the VM yet. `function_exported?/3` does not trigger
      # loading, so resolve_slug/1 used to silently stringify the atom to
      # "Elixir.PgFlow.TestFlows.UnloadableFlow" and blow up with an FK
      # violation on pgflow.flows.flow_slug.
      compile_flow(PgFlow.TestFlows.UnloadableFlow)
      true = :code.delete(PgFlow.TestFlows.UnloadableFlow)
      :code.purge(PgFlow.TestFlows.UnloadableFlow)
      refute function_exported?(PgFlow.TestFlows.UnloadableFlow, :__pgflow_slug__, 0)

      assert {:ok, run_id} =
               Client.start_flow(PgFlow.TestFlows.UnloadableFlow, %{"value" => 1})

      assert is_binary(run_id)
    end

    test "starts flow with string slug", %{flow_slug: flow_slug} do
      {:ok, run_id} = Client.start_flow(flow_slug, %{"value" => 42})
      assert is_binary(run_id)
    end

    test "starts flow with atom slug", %{flow_slug: _flow_slug} do
      {:ok, run_id} = Client.start_flow(:client_test_flow, %{"value" => 42})
      assert is_binary(run_id)
    end

    test "returns error for nonexistent flow" do
      result = Client.start_flow("nonexistent_flow", %{"value" => 1})
      assert {:error, _} = result
    end

    test "does not pre-validate legacy atom slugs" do
      result = Client.start_flow(:"my.flow", %{"value" => 1})

      assert {:error, _} = result
      refute match?({:error, {:invalid_slug, _}}, result)
    end
  end

  # ── repo resolution ────────────────────────────────────────────────

  describe "run reads" do
    test "preserve the public run contracts and load list-valued state output" do
      {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 42})

      TestRepo.query!(
        """
        UPDATE pgflow.step_states
        SET status = 'completed', remaining_tasks = 0, started_at = created_at,
            completed_at = created_at, output = '[1, 2, 3]'::jsonb
        WHERE run_id = $1 AND step_slug = 'process'
        """,
        [Ecto.UUID.dump!(run_id)]
      )

      assert {:ok, %PgFlow.Schema.Run{run_id: ^run_id}} = Client.get_run(run_id)

      assert {:ok, %PgFlow.Schema.Run{step_states: [state]}} =
               Client.get_run_with_states(run_id)

      assert state.output == [1, 2, 3]
      assert {:error, :invalid_id} = Client.get_run("not-a-uuid")
      assert {:error, :not_found} = Client.get_run(Ecto.UUID.generate())
    end
  end

  describe "repo resolution" do
    test "returns error when repo not configured" do
      :persistent_term.erase({PgFlow, :repo})
      old_env = Application.get_env(:pgflow, :repo)
      Application.delete_env(:pgflow, :repo)

      on_exit(fn ->
        if old_env, do: Application.put_env(:pgflow, :repo, old_env)
        :persistent_term.put({PgFlow, :repo}, TestRepo)
      end)

      assert {:error, "Repo not configured"} = Client.start_flow("some_flow", %{})
    end

    test "falls back to application env when persistent_term not set" do
      :persistent_term.erase({PgFlow, :repo})
      Application.put_env(:pgflow, :repo, TestRepo)

      on_exit(fn ->
        Application.delete_env(:pgflow, :repo)
        :persistent_term.put({PgFlow, :repo}, TestRepo)
      end)

      # Should not fail with "Repo not configured" — it should resolve the repo
      {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 42})
      assert is_binary(run_id)
    end
  end
end
