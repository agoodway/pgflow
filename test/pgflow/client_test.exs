defmodule PgFlow.ClientTest do
  use ExUnit.Case

  alias Ecto.Adapters.SQL.Sandbox
  alias PgFlow.Client
  alias PgFlow.Queries.{Flows, Signals, Workers}
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

  defp park_client_test_task(run_id) do
    worker_id = Ecto.UUID.generate()
    {:ok, _} = Workers.register_worker(TestRepo, worker_id, "client_test_flow", "elixir:test")
    {:ok, messages} = Flows.read(TestRepo, "client_test_flow", 30, 1)
    message_ids = Enum.map(messages, fn [message_id | _] -> message_id end)
    {:ok, _} = Flows.start_tasks(TestRepo, "client_test_flow", message_ids, worker_id)

    %{rows: [[attempts_count, message_id]]} =
      TestRepo.query!(
        """
        SELECT attempts_count, message_id
        FROM pgflow.step_tasks
        WHERE run_id = $1 AND step_slug = 'process' AND task_index = 0
        """,
        [Ecto.UUID.dump!(run_id)]
      )

    assert :parked =
             Signals.await_task_signal(
               TestRepo,
               run_id,
               "process",
               0,
               attempts_count,
               message_id,
               nil,
               true
             )
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

  # ── signal/3,4 ─────────────────────────────────────────────────────

  describe "signal/3 and signal/4" do
    test "returns the typed outcome for a buffered payload targeting an existing run" do
      {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 1})
      assert {:ok, :buffered} = Client.signal(run_id, :process, %{"decision" => "approved"})
    end

    test "returns missing for an unknown run without storing a row" do
      run_id = Ecto.UUID.generate()
      assert {:ok, :missing} = Client.signal(run_id, :process, %{"ok" => true})

      assert %{rows: [[0]]} =
               TestRepo.query!("SELECT count(*) FROM pgflow.task_signals WHERE run_id = $1", [
                 Ecto.UUID.dump!(run_id)
               ])
    end

    test "returns terminal for a terminal target without storing a signal row" do
      {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 1})

      TestRepo.query!(
        """
        UPDATE pgflow.step_states
        SET status = 'failed', failed_at = now()
        WHERE run_id = $1 AND step_slug = $2
        """,
        [Ecto.UUID.dump!(run_id), "process"]
      )

      assert {:ok, :terminal} =
               PgFlow.signal(run_id, :process, %{"decision" => "too_late"})

      assert %{rows: [[0]]} =
               TestRepo.query!(
                 """
                 SELECT count(*)
                 FROM pgflow.task_signals
                 WHERE run_id = $1 AND step_slug = $2 AND task_index = $3
                 """,
                 [Ecto.UUID.dump!(run_id), "process", 0]
               )
    end

    test "lists waiting tasks without exposing payloads or claim state" do
      {:ok, run_id} = Client.start_flow(ClientTestFlow, %{"value" => 1})
      park_client_test_task(run_id)

      assert {:ok, [waiting_task]} = Client.get_waiting_tasks(run_id)

      assert %{
               step_slug: "process",
               task_index: 0,
               wait_deadline_at: deadline,
               waiting_since: %DateTime{}
             } = waiting_task

      assert is_nil(deadline) or match?(%DateTime{}, deadline)

      assert Enum.sort(Map.keys(waiting_task)) ==
               Enum.sort([:step_slug, :task_index, :wait_deadline_at, :waiting_since])
    end
  end
end

defmodule PgFlow.ClientPublicContractTest do
  use ExUnit.Case, async: false

  alias PgFlow.Client

  setup do
    repo_key = {PgFlow, :repo}
    missing = make_ref()
    previous_persistent_repo = :persistent_term.get(repo_key, missing)
    previous_env_repo = Application.get_env(:pgflow, :repo, missing)

    :persistent_term.erase(repo_key)
    Application.delete_env(:pgflow, :repo)

    on_exit(fn ->
      if previous_persistent_repo == missing do
        :persistent_term.erase(repo_key)
      else
        :persistent_term.put(repo_key, previous_persistent_repo)
      end

      if previous_env_repo == missing do
        Application.delete_env(:pgflow, :repo)
      else
        Application.put_env(:pgflow, :repo, previous_env_repo)
      end
    end)

    :ok
  end

  test "signal validates the run UUID before resolving the repo" do
    assert {:error, :invalid_run_id} = Client.signal("not-a-uuid", :process, %{"ok" => true})
    assert {:error, :invalid_run_id} = PgFlow.signal("not-a-uuid", :process, %{"ok" => true})
  end

  test "signal returns the established repo configuration error" do
    assert {:error, "Repo not configured"} =
             Client.signal(Ecto.UUID.generate(), :process, %{"ok" => true})
  end

  test "waiting-task discovery validates UUIDs and returns configuration errors" do
    assert {:error, :invalid_run_id} = Client.get_waiting_tasks("not-a-uuid")
    assert {:error, "Repo not configured"} = Client.get_waiting_tasks(Ecto.UUID.generate())
  end
end
