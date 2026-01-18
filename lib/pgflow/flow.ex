defmodule PgFlow.Flow do
  @moduledoc """
  A macro-based DSL for defining pgflow workflows.

  This module provides a declarative way to define workflow steps with dependencies,
  retries, timeouts, and array processing capabilities. Use it by calling `use PgFlow.Flow`
  in your flow module.

  ## Example

      defmodule MyApp.Flows.Example do
        use PgFlow.Flow

        @flow slug: :example, max_attempts: 3, base_delay: 5, timeout: 60

        step :first do
          fn input, ctx ->
            %{result: input["value"] * 2}
          end
        end

        step :second, depends_on: [:first] do
          fn deps, ctx ->
            %{doubled: deps.first["result"]}
          end
        end

        map :process_items, array: :second do
          fn item, ctx ->
            %{processed: item}
          end
        end
      end

  ## Flow Options

  The `@flow` module attribute accepts the following options:

    * `:slug` - (required) atom identifier for the flow
    * `:max_attempts` - maximum retry attempts for failed steps (default: 1)
    * `:base_delay` - base delay in seconds for exponential backoff (default: 1)
    * `:timeout` - step execution timeout in seconds (default: 30)

  ## Step Options

  Steps defined with `step/2` or `step/3` accept these options:

    * `:depends_on` - list of step atoms this step depends on
    * `:handler` - module implementing PgFlow.StepHandler (alternative to block)
    * `:max_attempts` - override flow-level max_attempts
    * `:base_delay` - override flow-level base_delay
    * `:timeout` - override flow-level timeout
    * `:start_delay` - seconds to delay before starting this step

  ## Map Options

  Map steps defined with `map/2` or `map/3` accept step options plus:

    * `:array` - step slug whose output array to process (for dependent maps)

  ## Generated Functions

  Using this module generates the following callback functions:

    * `__pgflow_definition__/0` - returns a `PgFlow.Flow.Definition` struct
    * `__pgflow_slug__/0` - returns the flow slug atom
    * `__pgflow_steps__/0` - returns the raw step definitions
    * `__pgflow_handler__/1` - pattern-matched functions for each step

  """

  @doc """
  Defines a single execution step in the workflow.

  ## Examples

      # Basic step with inline handler
      step :fetch_data do
        fn input, ctx ->
          %{data: fetch_from_api(input["url"])}
        end
      end

      # Step with dependencies
      step :transform, depends_on: [:fetch_data] do
        fn deps, ctx ->
          %{transformed: transform(deps.fetch_data["data"])}
        end
      end

      # Step with module handler
      step :validate, handler: MyApp.ValidateHandler

      # Step with custom retry settings
      step :flaky_operation, max_attempts: 5, base_delay: 10 do
        fn input, ctx ->
          perform_operation()
        end
      end

  """
  defmacro step(slug, opts \\ [], do: block) do
    quote do
      @pgflow_steps {unquote(slug), :step, unquote(opts), unquote(Macro.escape(block))}
    end
  end

  @doc """
  Defines an array processing step that executes a handler for each item.

  ## Examples

      # Map over inline array
      map :process_users do
        fn user, ctx ->
          %{processed: process_user(user)}
        end
      end

      # Map over output from another step
      map :enrich_items, array: :fetch_items do
        fn item, ctx ->
          %{enriched: enrich(item)}
        end
      end

      # Map with module handler
      map :validate_each, array: :items, handler: MyApp.ValidateItemHandler

      # Map with custom settings
      map :slow_processing, array: :items, timeout: 120 do
        fn item, ctx ->
          %{result: slow_process(item)}
        end
      end

  """
  defmacro map(slug, opts \\ [], do: block) do
    quote do
      @pgflow_steps {unquote(slug), :map, unquote(opts), unquote(Macro.escape(block))}
    end
  end

  defmacro __using__(_opts) do
    quote do
      import PgFlow.Flow, only: [step: 2, step: 3, map: 2, map: 3]

      Module.register_attribute(__MODULE__, :flow, persist: false)
      Module.register_attribute(__MODULE__, :pgflow_steps, accumulate: true, persist: false)

      @before_compile PgFlow.Flow
    end
  end

  defmacro __before_compile__(env) do
    flow_attrs = Module.get_attribute(env.module, :flow)
    steps = Module.get_attribute(env.module, :pgflow_steps) |> Enum.reverse()

    unless flow_attrs do
      raise CompileError,
        file: env.file,
        line: env.line,
        description:
          "Missing @flow attribute. You must define @flow with at least a :slug option."
    end

    slug = Keyword.fetch!(flow_attrs, :slug)
    max_attempts = Keyword.get(flow_attrs, :max_attempts, 1)
    base_delay = Keyword.get(flow_attrs, :base_delay, 1)
    timeout = Keyword.get(flow_attrs, :timeout, 30)

    flow_opts = [
      max_attempts: max_attempts,
      base_delay: base_delay,
      timeout: timeout
    ]

    # Generate handler functions for each step
    handler_clauses = generate_handler_clauses(steps)

    # Generate step definitions
    step_defs = generate_step_definitions(steps, max_attempts, base_delay, timeout)

    quote do
      @doc """
      Returns the flow slug.
      """
      def __pgflow_slug__, do: unquote(slug)

      @doc """
      Returns the raw step definitions.
      """
      def __pgflow_steps__, do: unquote(Macro.escape(steps))

      @doc """
      Returns the flow definition struct.
      """
      def __pgflow_definition__ do
        %PgFlow.Flow.Definition{
          slug: unquote(slug),
          module: __MODULE__,
          steps: unquote(Macro.escape(step_defs)),
          opts: unquote(Macro.escape(flow_opts))
        }
      end

      @doc """
      Pattern-matched handler function for each step.
      """
      unquote_splicing(handler_clauses)

      # Catch-all clause for undefined steps
      def __pgflow_handler__(slug) do
        raise "No handler defined for step: #{inspect(slug)}"
      end
    end
  end

  # Generate handler function clauses for each step
  defp generate_handler_clauses(steps) do
    Enum.map(steps, fn {slug, _type, opts, block} ->
      generate_handler_clause(slug, opts, block)
    end)
  end

  defp generate_handler_clause(slug, opts, block) do
    if Keyword.has_key?(opts, :handler) do
      generate_module_handler(slug, Keyword.fetch!(opts, :handler))
    else
      generate_inline_handler(slug, block)
    end
  end

  defp generate_module_handler(slug, handler_module) do
    quote do
      def __pgflow_handler__(unquote(slug)) do
        fn input, context ->
          unquote(handler_module).handle(input, context)
        end
      end
    end
  end

  defp generate_inline_handler(slug, block) do
    quote do
      def __pgflow_handler__(unquote(slug)) do
        unquote(block)
      end
    end
  end

  # Generate step definition structs
  defp generate_step_definitions(steps, default_max_attempts, default_base_delay, default_timeout) do
    Enum.map(steps, fn {slug, type, opts, _block} ->
      max_attempts = Keyword.get(opts, :max_attempts, default_max_attempts)
      base_delay = Keyword.get(opts, :base_delay, default_base_delay)
      timeout = Keyword.get(opts, :timeout, default_timeout)
      start_delay = Keyword.get(opts, :start_delay, 0)
      array = Keyword.get(opts, :array)

      # For map steps with :array option, convert to depends_on
      depends_on =
        case {type, array} do
          {:map, source} when is_atom(source) and not is_nil(source) -> [source]
          _ -> Keyword.get(opts, :depends_on, [])
        end

      step_type =
        case type do
          :step -> :single
          :map -> :map
        end

      %PgFlow.Flow.Step{
        slug: slug,
        step_type: step_type,
        depends_on: depends_on,
        max_attempts: max_attempts,
        base_delay: base_delay,
        timeout: timeout,
        start_delay: start_delay
      }
    end)
  end
end
