defmodule PgFlow.Worker.Executor do
  @moduledoc """
  Task execution logic for flow steps.

  This module handles the execution of step handlers with proper timeout handling,
  output serialization, and error handling. It manages the complete lifecycle of
  a task execution:

  1. Building execution context with run metadata
  2. Retrieving the handler function from the flow module
  3. Preparing input data (flow input for root steps, deps for dependent steps)
  4. Executing the handler with timeout protection
  5. Serializing output as JSON-compatible data
  6. Converting errors to serializable format

  ## Handler Signatures

  Step handlers receive different inputs based on their dependencies:

  - **Root steps** (no dependencies): `fn input, ctx -> output end`
    - `input` is the flow input map
    - `ctx` is the `PgFlow.Context` struct

  - **Dependent steps**: `fn deps, ctx -> output end`
    - `deps` is a map of dependency outputs keyed by step slug
    - `ctx` is the `PgFlow.Context` struct

  ## Timeout Handling

  Task execution is protected by a configurable timeout. If a handler exceeds
  the timeout, it is terminated and the task is marked as failed.

  ## Output Serialization

  Handler outputs must be JSON-serializable. The executor validates and converts
  outputs to ensure they can be stored in the database.

  ## Usage

      task = %{
        run_id: "550e8400-e29b-41d4-a716-446655440000",
        step_slug: :process,
        task_index: 0,
        attempt: 1,
        input: %{"key" => "value"},
        deps: %{}
      }

      case PgFlow.Worker.Executor.execute(MyFlow, task, MyApp.Repo) do
        {:ok, output} ->
          # Task succeeded, persist output
          PgFlow.Queries.complete_task(repo, task.run_id, task.step_slug, task.task_index, output)

        {:error, error_message} ->
          # Task failed, persist error
          PgFlow.Queries.fail_task(repo, task.run_id, task.step_slug, task.task_index, error_message)
      end
  """

  alias PgFlow.Context

  @default_timeout 30_000

  @doc """
  Executes a flow step handler.

  ## Parameters

    * `flow_module` - The flow module containing the step handlers
    * `task` - Task map with execution details
    * `repo` - The Ecto repository
    * `opts` - Options keyword list

  ## Options

    * `:timeout` - Execution timeout in milliseconds (default: 30_000)

  ## Task Structure

  The task map must contain:
  - `:run_id` - Flow run UUID
  - `:step_slug` - Step identifier atom
  - `:task_index` - Task index (0 for single steps)
  - `:attempt` - Current attempt number
  - `:input` - Task input data map
  - `:deps` - Dependency outputs map

  ## Returns

    * `{:ok, output}` - Handler succeeded, output is JSON-serializable
    * `{:error, error_message}` - Handler failed or timed out

  ## Examples

      task = %{
        run_id: "550e8400-e29b-41d4-a716-446655440000",
        step_slug: :fetch_user,
        task_index: 0,
        attempt: 1,
        input: %{"user_id" => 123},
        deps: %{}
      }

      {:ok, output} = PgFlow.Worker.Executor.execute(MyFlow, task, MyApp.Repo)
      #=> {:ok, %{"name" => "John", "email" => "john@example.com"}}

      # Handler that times out
      {:error, msg} = PgFlow.Worker.Executor.execute(SlowFlow, task, MyApp.Repo, timeout: 100)
      #=> {:error, "Task timed out after 100ms"}
  """
  @spec execute(module(), map(), Ecto.Repo.t(), keyword()) ::
          {:ok, map()} | {:error, String.t()}
  def execute(flow_module, task, repo, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    # Note: Task lifecycle logging (started/completed/failed) is handled by Worker.Server
    # using PgFlow.Logger for structured output. This module focuses on execution.

    try do
      with {:ok, ctx} <- build_context(task, repo),
           {:ok, handler} <- get_handler(flow_module, task.step_slug),
           {:ok, handler_input} <- prepare_input(task, ctx),
           {:ok, output} <- execute_with_timeout(handler, handler_input, ctx, timeout),
           {:ok, serialized_output} <- serialize_output(output) do
        {:ok, serialized_output}
      else
        {:error, reason} ->
          {:error, serialize_error(reason)}
      end
    rescue
      error ->
        {:error, serialize_error(error)}
    end
  end

  @doc """
  Builds a PgFlow.Context struct for task execution.

  The context provides step handlers with metadata about the current execution
  environment and utilities for accessing flow data.

  ## Parameters

    * `task` - Task map with execution details
    * `repo` - The Ecto repository

  ## Returns

    * `{:ok, context}` - Context struct ready for handler execution
    * `{:error, reason}` - Error if context creation fails

  ## Examples

      task = %{
        run_id: "550e8400-e29b-41d4-a716-446655440000",
        step_slug: :process,
        task_index: 0,
        attempt: 1,
        input: %{},
        deps: %{}
      }

      {:ok, ctx} = PgFlow.Worker.Executor.build_context(task, MyApp.Repo)
      #=> {:ok, %PgFlow.Context{
      #=>   run_id: "550e8400-e29b-41d4-a716-446655440000",
      #=>   step_slug: :process,
      #=>   task_index: 0,
      #=>   attempt: 1,
      #=>   repo: MyApp.Repo,
      #=>   flow_input: :not_loaded
      #=> }}
  """
  @spec build_context(map(), Ecto.Repo.t()) :: {:ok, Context.t()} | {:error, term()}
  def build_context(task, repo) do
    ctx =
      Context.new(
        run_id: task.run_id,
        step_slug: task.step_slug,
        task_index: task.task_index,
        attempt: task.attempt,
        repo: repo
      )

    {:ok, ctx}
  rescue
    error ->
      {:error, "Failed to build context: #{inspect(error)}"}
  end

  @doc """
  Retrieves the handler function for a step.

  Calls the flow module's `__pgflow_handler__/1` function to get the
  handler function for the specified step.

  ## Parameters

    * `flow_module` - The flow module
    * `step_slug` - Step identifier atom

  ## Returns

    * `{:ok, handler}` - Handler function
    * `{:error, reason}` - Error if handler not found

  ## Examples

      {:ok, handler} = PgFlow.Worker.Executor.get_handler(MyFlow, :process)
      #=> {:ok, #Function<...>}

      {:error, msg} = PgFlow.Worker.Executor.get_handler(MyFlow, :nonexistent)
      #=> {:error, "No handler defined for step: :nonexistent"}
  """
  @spec get_handler(module(), atom()) :: {:ok, function()} | {:error, String.t()}
  def get_handler(flow_module, step_slug) do
    handler = flow_module.__pgflow_handler__(step_slug)
    {:ok, handler}
  rescue
    error ->
      {:error, "Failed to get handler for step #{inspect(step_slug)}: #{inspect(error)}"}
  end

  @doc """
  Prepares input data for handler execution.

  For root steps (no dependencies), returns the task input directly.
  For dependent steps, returns the deps map containing outputs from dependencies.

  ## Parameters

    * `task` - Task map with input and deps
    * `ctx` - Execution context (currently unused, for future extensions)

  ## Returns

    * `{:ok, handler_input}` - Input data for the handler
    * `{:error, reason}` - Error if input preparation fails

  ## Examples

      # Root step - uses input
      task = %{input: %{"key" => "value"}, deps: %{}}
      {:ok, input} = PgFlow.Worker.Executor.prepare_input(task, ctx)
      #=> {:ok, %{"key" => "value"}}

      # Dependent step - uses deps
      task = %{input: %{}, deps: %{fetch: %{"result" => 123}}}
      {:ok, input} = PgFlow.Worker.Executor.prepare_input(task, ctx)
      #=> {:ok, %{fetch: %{"result" => 123}}}
  """
  @spec prepare_input(map(), Context.t()) :: {:ok, map()} | {:error, term()}
  def prepare_input(%{deps: deps, input: _input}, _ctx) when map_size(deps) > 0 do
    # Dependent step - use deps
    {:ok, deps}
  end

  def prepare_input(%{input: input, deps: _deps}, _ctx) do
    # Root step - use input
    {:ok, input}
  end

  @doc """
  Executes a handler function with timeout protection.

  Spawns the handler in a separate task and awaits the result with the
  specified timeout. If the timeout is exceeded, the task is killed and
  an error is returned.

  ## Parameters

    * `handler` - Handler function to execute
    * `input` - Input data for the handler
    * `ctx` - Execution context
    * `timeout` - Timeout in milliseconds

  ## Returns

    * `{:ok, output}` - Handler completed successfully
    * `{:error, reason}` - Handler failed or timed out

  ## Examples

      handler = fn input, _ctx -> %{result: input["value"] * 2} end
      input = %{"value" => 21}

      {:ok, output} = PgFlow.Worker.Executor.execute_with_timeout(handler, input, ctx, 5000)
      #=> {:ok, %{result: 42}}

      slow_handler = fn _input, _ctx -> Process.sleep(10_000); %{} end
      {:error, msg} = PgFlow.Worker.Executor.execute_with_timeout(slow_handler, input, ctx, 100)
      #=> {:error, "Task timed out after 100ms"}
  """
  @spec execute_with_timeout(function(), map(), Context.t(), pos_integer()) ::
          {:ok, term()} | {:error, String.t()}
  def execute_with_timeout(handler, input, ctx, timeout) do
    task =
      Task.async(fn ->
        handler.(input, ctx)
      end)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, result} ->
        {:ok, result}

      {:exit, reason} ->
        {:error, "Handler exited: #{inspect(reason)}"}

      nil ->
        {:error, "Task timed out after #{timeout}ms"}
    end
  rescue
    error ->
      {:error, "Handler execution failed: #{inspect(error)}"}
  end

  @doc """
  Serializes handler output to ensure JSON compatibility.

  Validates that the output can be encoded as JSON. This ensures outputs
  can be stored in the database and passed to dependent steps.

  ## Parameters

    * `output` - Handler output to serialize

  ## Returns

    * `{:ok, output}` - Output is JSON-serializable
    * `{:error, reason}` - Output cannot be serialized

  ## Examples

      {:ok, output} = PgFlow.Worker.Executor.serialize_output(%{key: "value"})
      #=> {:ok, %{key: "value"}}

      {:ok, output} = PgFlow.Worker.Executor.serialize_output([1, 2, 3])
      #=> {:ok, [1, 2, 3]}

      {:error, msg} = PgFlow.Worker.Executor.serialize_output(%{pid: self()})
      #=> {:error, "Output is not JSON-serializable: ..."}
  """
  @spec serialize_output(term()) :: {:ok, map() | list()} | {:error, String.t()}
  def serialize_output(output) when is_map(output) or is_list(output) do
    case Jason.encode(output) do
      {:ok, _json} ->
        {:ok, output}

      {:error, reason} ->
        {:error, "Output is not JSON-serializable: #{inspect(reason)}"}
    end
  end

  def serialize_output(output) do
    {:error, "Output must be a map or list, got: #{inspect(output)}"}
  end

  @doc """
  Converts an error to a serializable string representation.

  Handles various error types including strings, atoms, exceptions, and
  complex terms. Ensures errors can be persisted to the database.

  ## Parameters

    * `error` - Error value to serialize

  ## Returns

  A string representation of the error.

  ## Examples

      PgFlow.Worker.Executor.serialize_error("Connection failed")
      #=> "Connection failed"

      PgFlow.Worker.Executor.serialize_error(:timeout)
      #=> "timeout"

      PgFlow.Worker.Executor.serialize_error(%RuntimeError{message: "Something broke"})
      #=> "RuntimeError: Something broke"

      PgFlow.Worker.Executor.serialize_error({:error, :not_found})
      #=> "{:error, :not_found}"
  """
  @spec serialize_error(term()) :: String.t()
  def serialize_error(error) when is_binary(error), do: error
  def serialize_error(error) when is_atom(error), do: Atom.to_string(error)

  def serialize_error(%{__struct__: struct_name, __exception__: true, message: message}) do
    "#{inspect(struct_name)}: #{message}"
  end

  def serialize_error(%{__struct__: _, __exception__: true} = error) do
    Exception.message(error)
  end

  def serialize_error(error) do
    inspect(error)
  end
end
