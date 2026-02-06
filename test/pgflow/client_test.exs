defmodule PgFlow.ClientTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.TestRepo
  alias PgFlow.Client

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
  end

  # ── repo resolution ────────────────────────────────────────────────

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
