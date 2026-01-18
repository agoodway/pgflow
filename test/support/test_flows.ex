defmodule PgFlow.TestFlows do
  @moduledoc """
  Example flows for testing the PgFlow framework.
  """

  defmodule SimpleFlow do
    @moduledoc """
    A single-step flow for testing basic functionality.
    """
    use PgFlow.Flow

    @flow slug: :simple_flow, max_attempts: 3

    step :process do
      fn input, _ctx ->
        %{result: input["value"] * 2}
      end
    end
  end

  defmodule LinearFlow do
    @moduledoc """
    A linear A -> B -> C flow for testing dependencies.
    """
    use PgFlow.Flow

    @flow slug: :linear_flow, max_attempts: 3

    step :step_a do
      fn input, _ctx ->
        %{a_value: input["value"] + 1}
      end
    end

    step :step_b, depends_on: [:step_a] do
      fn deps, _ctx ->
        %{b_value: deps.step_a["a_value"] + 10}
      end
    end

    step :step_c, depends_on: [:step_b] do
      fn deps, _ctx ->
        %{c_value: deps.step_b["b_value"] + 100}
      end
    end
  end

  defmodule ParallelFlow do
    @moduledoc """
    A flow with parallel steps that fan-out and fan-in.

        start
          |
        step_a
        /    \
    step_b  step_c
        \    /
        step_d
    """
    use PgFlow.Flow

    @flow slug: :parallel_flow, max_attempts: 3

    step :step_a do
      fn input, _ctx ->
        %{value: input["value"]}
      end
    end

    step :step_b, depends_on: [:step_a] do
      fn deps, _ctx ->
        %{b_value: deps.step_a["value"] * 2}
      end
    end

    step :step_c, depends_on: [:step_a] do
      fn deps, _ctx ->
        %{c_value: deps.step_a["value"] * 3}
      end
    end

    step :step_d, depends_on: [:step_b, :step_c] do
      fn deps, _ctx ->
        %{total: deps.step_b["b_value"] + deps.step_c["c_value"]}
      end
    end
  end

  defmodule MapFlow do
    @moduledoc """
    A flow with map steps for array processing.
    """
    use PgFlow.Flow

    @flow slug: :map_flow, max_attempts: 3

    # Root map - input should be an array
    map :process_items do
      fn item, _ctx ->
        %{processed: item * 2}
      end
    end

    step :aggregate, depends_on: [:process_items] do
      fn deps, _ctx ->
        total =
          Enum.reduce(deps.process_items, 0, fn item, acc ->
            acc + item["processed"]
          end)

        %{total: total}
      end
    end
  end

  defmodule DependentMapFlow do
    @moduledoc """
    A flow with a dependent map step that processes output from another step.
    """
    use PgFlow.Flow

    @flow slug: :dependent_map_flow, max_attempts: 3

    step :generate_items do
      fn input, _ctx ->
        count = input["count"] || 3
        Enum.to_list(1..count)
      end
    end

    map :process_items, array: :generate_items do
      fn item, _ctx ->
        %{squared: item * item}
      end
    end

    step :summarize, depends_on: [:process_items] do
      fn deps, _ctx ->
        sum =
          Enum.reduce(deps.process_items, 0, fn item, acc ->
            acc + item["squared"]
          end)

        %{sum_of_squares: sum}
      end
    end
  end

  defmodule FailingFlow do
    @moduledoc """
    A flow that fails for testing error handling.
    """
    use PgFlow.Flow

    @flow slug: :failing_flow, max_attempts: 2, base_delay: 1

    step :will_fail do
      fn _input, _ctx ->
        raise "Intentional failure for testing"
      end
    end
  end

  defmodule ConditionalFailFlow do
    @moduledoc """
    A flow that fails conditionally based on input.
    """
    use PgFlow.Flow

    @flow slug: :conditional_fail_flow, max_attempts: 3

    step :check_and_process do
      fn input, _ctx ->
        if input["should_fail"] do
          raise "Conditional failure triggered"
        else
          %{success: true, value: input["value"]}
        end
      end
    end
  end

  defmodule RetryFlow do
    @moduledoc """
    A flow that succeeds after retries.
    Uses process dictionary to track attempts (for testing only).
    """
    use PgFlow.Flow

    @flow slug: :retry_flow, max_attempts: 3, base_delay: 1

    step :retry_step do
      fn input, ctx ->
        key = {:retry_attempt, ctx.run_id}
        attempts = Process.get(key, 0) + 1
        Process.put(key, attempts)

        succeed_on = input["succeed_on_attempt"] || 2

        if attempts >= succeed_on do
          %{success: true, attempts: attempts}
        else
          raise "Attempt #{attempts} failed, need #{succeed_on}"
        end
      end
    end
  end

  defmodule TimeoutFlow do
    @moduledoc """
    A flow with a step that times out.
    """
    use PgFlow.Flow

    @flow slug: :timeout_flow, max_attempts: 1, timeout: 1

    step :slow_step do
      fn _input, _ctx ->
        Process.sleep(5000)
        %{completed: true}
      end
    end
  end
end
