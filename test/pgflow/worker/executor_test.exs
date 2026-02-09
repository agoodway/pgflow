defmodule PgFlow.Worker.ExecutorTest do
  use ExUnit.Case, async: true

  alias PgFlow.TestFlows.FailingFlow
  alias PgFlow.TestFlows.SimpleFlow
  alias PgFlow.Worker.Executor

  defp build_task(overrides \\ %{}) do
    Map.merge(
      %{
        run_id: Ecto.UUID.generate(),
        step_slug: :process,
        task_index: 0,
        attempt: 1,
        input: %{"value" => 42},
        deps: %{}
      },
      overrides
    )
  end

  describe "serialize_output/1" do
    test "accepts simple map" do
      assert {:ok, %{key: "value"}} = Executor.serialize_output(%{key: "value"})
    end

    test "accepts nested map" do
      nested = %{outer: %{inner: "deep"}}
      assert {:ok, ^nested} = Executor.serialize_output(nested)
    end

    test "accepts list of maps" do
      list = [%{a: 1}, %{b: 2}]
      assert {:ok, ^list} = Executor.serialize_output(list)
    end

    test "rejects map containing PID" do
      assert {:error, msg} = Executor.serialize_output(%{pid: self()})
      assert msg =~ "not JSON-serializable"
    end

    test "rejects bare integer" do
      assert {:error, msg} = Executor.serialize_output(42)
      assert msg =~ "must be a map or list"
    end

    test "rejects bare string" do
      assert {:error, msg} = Executor.serialize_output("hello")
      assert msg =~ "must be a map or list"
    end

    test "rejects tuple" do
      assert {:error, msg} = Executor.serialize_output({:ok, "result"})
      assert msg =~ "must be a map or list"
    end
  end

  describe "serialize_error/1" do
    test "string returns as-is" do
      assert Executor.serialize_error("Connection failed") == "Connection failed"
    end

    test "atom converts to string" do
      assert Executor.serialize_error(:timeout) == "timeout"
    end

    test "exception struct returns Module: message" do
      error = %RuntimeError{message: "Something broke"}
      assert Executor.serialize_error(error) == "RuntimeError: Something broke"
    end

    test "other term returns inspect" do
      assert Executor.serialize_error({:error, :not_found}) == "{:error, :not_found}"
    end
  end

  describe "prepare_input/2" do
    test "empty deps returns input (root step)" do
      task = build_task(%{input: %{"value" => 42}, deps: %{}})
      ctx = build_context(task)
      assert {:ok, %{"value" => 42}} = Executor.prepare_input(task, ctx)
    end

    test "non-empty deps returns deps (dependent step)" do
      deps = %{step_a: %{"result" => 100}}
      task = build_task(%{deps: deps})
      ctx = build_context(task)
      assert {:ok, ^deps} = Executor.prepare_input(task, ctx)
    end
  end

  describe "build_context/2" do
    test "returns {:ok, context} with correct fields" do
      task = build_task()
      {:ok, ctx} = Executor.build_context(task, FakeRepo)

      assert ctx.run_id == task.run_id
      assert ctx.step_slug == :process
      assert ctx.task_index == 0
      assert ctx.attempt == 1
      assert ctx.repo == FakeRepo
      assert ctx.flow_input == :not_loaded
    end
  end

  describe "get_handler/2" do
    test "returns {:ok, fn} for existing step" do
      {:ok, handler} = Executor.get_handler(SimpleFlow, :process)
      assert is_function(handler, 2)
    end

    test "returns {:error, msg} for nonexistent step" do
      {:error, msg} = Executor.get_handler(SimpleFlow, :nonexistent)
      assert msg =~ "handler"
      assert msg =~ "nonexistent"
    end
  end

  describe "execute_with_timeout/4" do
    test "returns {:ok, result} for fast handler" do
      handler = fn input, _ctx -> %{doubled: input["value"] * 2} end
      input = %{"value" => 21}
      ctx = build_context(build_task())

      assert {:ok, %{doubled: 42}} = Executor.execute_with_timeout(handler, input, ctx, 5000)
    end

    test "returns {:error, msg} when handler raises" do
      Process.flag(:trap_exit, true)
      handler = fn _input, _ctx -> raise "boom" end
      ctx = build_context(build_task())

      assert {:error, msg} = Executor.execute_with_timeout(handler, %{}, ctx, 5000)
      assert msg =~ "exited" or msg =~ "boom"
    end

    test "returns {:error, timeout_msg} when handler exceeds timeout" do
      handler = fn _input, _ctx -> Process.sleep(5000) end
      ctx = build_context(build_task())

      assert {:error, msg} = Executor.execute_with_timeout(handler, %{}, ctx, 50)
      assert msg =~ "timed out after 50ms"
    end
  end

  describe "execute/4" do
    test "happy path: valid flow module + task map returns {:ok, output}" do
      task = build_task(%{input: %{"value" => 21}, deps: %{}})

      assert {:ok, output} = Executor.execute(SimpleFlow, task, FakeRepo)
      assert output == %{result: 42}
    end

    test "error path: failing handler returns {:error, error_string}" do
      Process.flag(:trap_exit, true)
      task = build_task(%{step_slug: :will_fail, input: %{}, deps: %{}})

      assert {:error, error_msg} = Executor.execute(FailingFlow, task, FakeRepo)
      assert is_binary(error_msg)
      assert error_msg =~ "Intentional failure"
    end
  end

  # Helper to build a minimal context for prepare_input tests
  defp build_context(task) do
    PgFlow.Context.new(
      run_id: task.run_id,
      step_slug: task.step_slug,
      task_index: task.task_index,
      attempt: task.attempt,
      repo: FakeRepo
    )
  end
end
