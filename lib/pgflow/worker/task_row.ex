defmodule PgFlow.Worker.TaskRow do
  @moduledoc """
  Decodes one `pgflow.step_task_record` row into the fields the worker dispatches on.

  The row shape is a positional list, so it is decoded in one place rather than
  pattern-matched at the call site.

  ## Shape

  Queue-aware `pgflow.start_tasks/4` returns eight columns:

      [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input, attempts_count]

  `attempts_count` must be present; a null is a decode error so supported startup
  never silently treats a missing count as the first attempt.
  """

  @type t :: %{
          flow_slug: String.t(),
          run_id: binary(),
          step_slug: String.t(),
          input: term(),
          msg_id: integer(),
          task_index: non_neg_integer(),
          flow_input: term() | nil,
          attempt: pos_integer()
        }

  @expected_columns 8

  @doc """
  Decodes an eight-column `step_task_record` row.

  Returns the row's fields keyed by name, with `:attempt` always a positive
  integer. Legacy seven-column rows are rejected — supported startup requires
  helpers that return `attempts_count`.
  """
  @spec decode([term()]) :: t()
  def decode(row) when is_list(row) and length(row) == @expected_columns do
    [
      flow_slug,
      run_id,
      step_slug,
      input,
      msg_id,
      task_index,
      flow_input,
      attempts_count
    ] = row

    if is_nil(attempts_count) do
      raise ArgumentError, "expected non-null attempts_count in step_task_record"
    end

    %{
      flow_slug: flow_slug,
      run_id: run_id,
      step_slug: step_slug,
      input: input,
      msg_id: msg_id,
      task_index: task_index,
      flow_input: flow_input,
      attempt: attempts_count
    }
  end

  def decode(row) when is_list(row) do
    raise ArgumentError,
          "expected #{@expected_columns}-column step_task_record, got #{length(row)} columns"
  end
end
