defmodule PgFlow.Worker.TaskRow do
  @moduledoc """
  Decodes one `pgflow.step_task_record` row into the fields the worker dispatches on.

  The row shape is a positional list, so it is decoded in one place rather than
  pattern-matched at the call site — that is what lets the worker straddle a
  schema upgrade.

  ## Two shapes

  `pgflow_helpers` v03 appends `attempts_count` to the composite type, so
  `pgflow.start_tasks` returns eight columns instead of seven. A worker deployed
  ahead of that migration — the order this change has to ship in, since the
  reverse breaks every dispatch — still receives seven, and reports `attempt: 1`
  for them. That is exactly the value the worker hardcoded before v03, so the
  un-migrated path behaves as it always did.

      [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]
      [flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input, attempts_count]

  `attempts_count` is nullable, and a null is read as the first attempt for the
  same reason: a missing count must not present as a task that has never run.
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

  @first_attempt 1

  @doc """
  Decodes a seven- or eight-column `step_task_record` row.

  Returns the row's fields keyed by name, with `:attempt` always a positive
  integer.
  """
  @spec decode([term()]) :: t()
  def decode([flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input]) do
    decode([flow_slug, run_id, step_slug, input, msg_id, task_index, flow_input, @first_attempt])
  end

  def decode([
        flow_slug,
        run_id,
        step_slug,
        input,
        msg_id,
        task_index,
        flow_input,
        attempts_count
      ]) do
    %{
      flow_slug: flow_slug,
      run_id: run_id,
      step_slug: step_slug,
      input: input,
      msg_id: msg_id,
      task_index: task_index,
      flow_input: flow_input,
      attempt: attempts_count || @first_attempt
    }
  end
end
