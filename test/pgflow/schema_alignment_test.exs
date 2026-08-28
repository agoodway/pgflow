defmodule PgFlow.SchemaAlignmentTest do
  use ExUnit.Case, async: true

  alias PgFlow.Schema.{Dep, Flow, Run, Step, StepState, StepTask, Worker}
  alias PgFlow.Type.JSON

  test "run fields match the pgflow.runs columns" do
    assert_schema(Run,
      run_id: :binary_id,
      flow_slug: :string,
      status: :string,
      input: JSON,
      output: JSON,
      remaining_steps: :integer,
      started_at: :utc_datetime_usec,
      completed_at: :utc_datetime_usec,
      failed_at: :utc_datetime_usec
    )
  end

  test "step fields match the pgflow.steps columns" do
    assert_schema(Step,
      flow_slug: :string,
      step_slug: :string,
      step_type: :string,
      step_index: :integer,
      deps_count: :integer,
      opt_max_attempts: :integer,
      opt_base_delay: :integer,
      opt_timeout: :integer,
      created_at: :utc_datetime_usec,
      opt_start_delay: :integer,
      required_input_pattern: :map,
      forbidden_input_pattern: :map,
      when_unmet: :string,
      when_exhausted: :string
    )
  end

  test "dependency fields match the pgflow.deps columns and primary key order" do
    assert_schema(Dep,
      flow_slug: :string,
      dep_slug: :string,
      step_slug: :string,
      created_at: :utc_datetime_usec
    )

    assert Dep.__schema__(:primary_key) == [:flow_slug, :dep_slug, :step_slug]
  end

  test "schemas do not expose single-column associations for composite foreign keys" do
    refute :deps in Step.__schema__(:associations)
    refute :step_states in Step.__schema__(:associations)
    refute :step in Dep.__schema__(:associations)
    refute :step in StepState.__schema__(:associations)
    refute :step_tasks in StepState.__schema__(:associations)
    refute :step_state in StepTask.__schema__(:associations)
  end

  test "step state fields match the pgflow.step_states columns" do
    assert_schema(StepState,
      flow_slug: :string,
      run_id: :binary_id,
      step_slug: :string,
      status: :string,
      remaining_tasks: :integer,
      remaining_deps: :integer,
      error_message: :string,
      initial_tasks: :integer,
      created_at: :utc_datetime_usec,
      started_at: :utc_datetime_usec,
      completed_at: :utc_datetime_usec,
      failed_at: :utc_datetime_usec,
      output: JSON,
      skip_reason: :string,
      skipped_at: :utc_datetime_usec
    )
  end

  test "step task fields match the pgflow.step_tasks columns" do
    assert_schema(StepTask,
      flow_slug: :string,
      run_id: :binary_id,
      step_slug: :string,
      message_id: :integer,
      task_index: :integer,
      status: :string,
      attempts_count: :integer,
      error_message: :string,
      output: JSON,
      queued_at: :utc_datetime_usec,
      completed_at: :utc_datetime_usec,
      failed_at: :utc_datetime_usec,
      started_at: :utc_datetime_usec,
      last_worker_id: :binary_id,
      requeued_count: :integer,
      last_requeued_at: :utc_datetime_usec,
      permanently_stalled_at: :utc_datetime_usec
    )

    refute :input in StepTask.__schema__(:fields)
  end

  test "worker fields match the pgflow.workers columns" do
    assert_schema(Worker,
      worker_id: :binary_id,
      queue_name: :string,
      function_name: :string,
      started_at: :utc_datetime_usec,
      deprecated_at: :utc_datetime_usec,
      last_heartbeat_at: :utc_datetime_usec,
      stopped_at: :utc_datetime_usec
    )
  end

  test "flow fields match the pgflow.flows columns" do
    assert_schema(Flow,
      flow_slug: :string,
      opt_max_attempts: :integer,
      opt_base_delay: :integer,
      opt_timeout: :integer,
      created_at: :utc_datetime_usec,
      flow_type: :string
    )
  end

  defp assert_schema(schema, expected_fields) do
    assert schema.__schema__(:fields) == Keyword.keys(expected_fields)

    Enum.each(expected_fields, fn {field, type} ->
      assert schema.__schema__(:type, field) == type
    end)
  end
end
