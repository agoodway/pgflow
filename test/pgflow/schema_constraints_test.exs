defmodule PgFlow.SchemaConstraintsTest do
  use PgFlow.IntegrationCase

  alias PgFlow.Schema.{Dep, StepState, StepTask}

  describe "composite foreign keys" do
    test "dependency changesets map a missing dependency step to an error" do
      create_flow("dependency_constraint")
      add_step("dependency_constraint", "target")

      changeset =
        Dep.changeset(%Dep{}, %{
          flow_slug: "dependency_constraint",
          dep_slug: "missing_dependency",
          step_slug: "target"
        })

      assert {:error, changeset} = TestRepo.insert(changeset)

      assert {"does not exist",
              constraint: :foreign, constraint_name: "deps_flow_slug_dep_slug_fkey"} =
               changeset.errors[:dep_slug]
    end

    test "dependency changesets map a missing target step to an error" do
      create_flow("target_constraint")
      add_step("target_constraint", "dependency")

      changeset =
        Dep.changeset(%Dep{}, %{
          flow_slug: "target_constraint",
          dep_slug: "dependency",
          step_slug: "missing_target"
        })

      assert {:error, changeset} = TestRepo.insert(changeset)

      assert {"does not exist",
              constraint: :foreign, constraint_name: "deps_flow_slug_step_slug_fkey"} =
               changeset.errors[:step_slug]
    end

    test "step state changesets map a missing flow step to an error" do
      create_flow("step_state_constraint")
      run_id = start_flow_run("step_state_constraint", %{})

      changeset =
        StepState.changeset(%StepState{}, %{
          flow_slug: "step_state_constraint",
          run_id: run_id,
          step_slug: "missing_step",
          status: "created"
        })

      assert {:error, changeset} = TestRepo.insert(changeset)

      assert {"does not exist",
              constraint: :foreign, constraint_name: "step_states_flow_slug_step_slug_fkey"} =
               changeset.errors[:step_slug]
    end

    test "step task changesets map a missing run step state to an error" do
      create_flow("step_task_constraint")
      add_step("step_task_constraint", "existing_step")
      run_id = start_flow_run("step_task_constraint", %{})

      changeset =
        StepTask.changeset(%StepTask{}, %{
          flow_slug: "step_task_constraint",
          run_id: run_id,
          step_slug: "missing_step",
          task_index: 0,
          status: "queued"
        })

      assert {:error, changeset} = TestRepo.insert(changeset)

      assert {"does not exist",
              constraint: :foreign, constraint_name: "step_tasks_run_id_step_slug_fkey"} =
               changeset.errors[:step_slug]
    end
  end
end
