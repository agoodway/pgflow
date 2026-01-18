defmodule PgFlow.ContextTest do
  use ExUnit.Case, async: true

  alias PgFlow.Context

  describe "struct creation" do
    test "requires all enforced keys" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        struct!(Context, [])
      end
    end

    test "requires :run_id" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        struct!(Context,
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )
      end
    end

    test "requires :step_slug" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )
      end
    end

    test "requires :task_index" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          attempt: 1,
          repo: TestRepo
        )
      end
    end

    test "requires :attempt" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          repo: TestRepo
        )
      end
    end

    test "requires :repo" do
      assert_raise ArgumentError, ~r/the following keys must also be given/, fn ->
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1
        )
      end
    end

    test "creates context with all required fields" do
      ctx =
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.run_id == "550e8400-e29b-41d4-a716-446655440000"
      assert ctx.step_slug == :test
      assert ctx.task_index == 0
      assert ctx.attempt == 1
      assert ctx.repo == TestRepo
    end

    test "flow_input defaults to :not_loaded" do
      ctx =
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.flow_input == :not_loaded
    end

    test "can override flow_input" do
      input = %{"key" => "value"}

      ctx =
        struct!(Context,
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo,
          flow_input: input
        )

      assert ctx.flow_input == input
    end
  end

  describe "new/1" do
    test "creates context from keyword list" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert %Context{} = ctx
      assert ctx.run_id == "550e8400-e29b-41d4-a716-446655440000"
      assert ctx.step_slug == :process
      assert ctx.task_index == 0
      assert ctx.attempt == 1
      assert ctx.repo == TestRepo
    end

    test "sets flow_input to :not_loaded by default" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.flow_input == :not_loaded
    end

    test "accepts flow_input in options" do
      input = %{"order_id" => 123}

      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo,
          flow_input: input
        )

      assert ctx.flow_input == input
    end

    test "raises on missing required keys" do
      assert_raise ArgumentError, fn ->
        Context.new(step_slug: :process)
      end
    end
  end

  describe "field access" do
    setup do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 5,
          attempt: 2,
          repo: TestRepo
        )

      {:ok, ctx: ctx}
    end

    test "accesses run_id", %{ctx: ctx} do
      assert ctx.run_id == "550e8400-e29b-41d4-a716-446655440000"
    end

    test "accesses step_slug", %{ctx: ctx} do
      assert ctx.step_slug == :process
    end

    test "accesses task_index", %{ctx: ctx} do
      assert ctx.task_index == 5
    end

    test "accesses attempt", %{ctx: ctx} do
      assert ctx.attempt == 2
    end

    test "accesses repo", %{ctx: ctx} do
      assert ctx.repo == TestRepo
    end

    test "accesses flow_input", %{ctx: ctx} do
      assert ctx.flow_input == :not_loaded
    end
  end

  describe "pattern matching" do
    test "can pattern match on run_id" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert %Context{run_id: "550e8400-e29b-41d4-a716-446655440000"} = ctx
    end

    test "can pattern match on step_slug" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert %Context{step_slug: :process} = ctx
    end

    test "can pattern match on attempt" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 3,
          repo: TestRepo
        )

      assert %Context{attempt: 3} = ctx
    end

    test "can pattern match on flow_input status" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert %Context{flow_input: :not_loaded} = ctx
    end
  end

  describe "struct updates" do
    setup do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      {:ok, ctx: ctx}
    end

    test "can update step_slug", %{ctx: ctx} do
      updated = %{ctx | step_slug: :new_step}
      assert updated.step_slug == :new_step
    end

    test "can update task_index", %{ctx: ctx} do
      updated = %{ctx | task_index: 10}
      assert updated.task_index == 10
    end

    test "can update attempt", %{ctx: ctx} do
      updated = %{ctx | attempt: 5}
      assert updated.attempt == 5
    end

    test "can update flow_input", %{ctx: ctx} do
      input = %{"key" => "value"}
      updated = %{ctx | flow_input: input}
      assert updated.flow_input == input
    end

    test "preserves other fields when updating", %{ctx: ctx} do
      updated = %{ctx | attempt: 2}

      assert updated.run_id == ctx.run_id
      assert updated.step_slug == ctx.step_slug
      assert updated.task_index == ctx.task_index
      assert updated.repo == ctx.repo
      assert updated.attempt == 2
    end
  end

  describe "context with different field values" do
    test "accepts UUID v4 format run_id" do
      ctx =
        Context.new(
          run_id: "a5f3c7e2-9b1d-4c8e-a3f6-2d7b5e8c9f1a",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.run_id == "a5f3c7e2-9b1d-4c8e-a3f6-2d7b5e8c9f1a"
    end

    test "accepts various step_slug atoms" do
      slugs = [:fetch, :process, :validate, :send_email, :cleanup]

      for slug <- slugs do
        ctx =
          Context.new(
            run_id: "550e8400-e29b-41d4-a716-446655440000",
            step_slug: slug,
            task_index: 0,
            attempt: 1,
            repo: TestRepo
          )

        assert ctx.step_slug == slug
      end
    end

    test "accepts zero task_index" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.task_index == 0
    end

    test "accepts positive task_index" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 99,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.task_index == 99
    end

    test "accepts attempt starting at 1" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.attempt == 1
    end

    test "accepts higher attempt numbers" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 5,
          repo: TestRepo
        )

      assert ctx.attempt == 5
    end

    test "accepts different repo modules" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: MyApp.Repo
        )

      assert ctx.repo == MyApp.Repo
    end
  end

  describe "context with preloaded flow_input" do
    test "creates context with map flow_input" do
      input = %{"order_id" => 123, "customer_id" => 456}

      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo,
          flow_input: input
        )

      assert ctx.flow_input == input
    end

    test "creates context with empty map flow_input" do
      input = %{}

      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo,
          flow_input: input
        )

      assert ctx.flow_input == input
    end

    test "creates context with nested map flow_input" do
      input = %{
        "user" => %{
          "id" => 123,
          "email" => "test@example.com"
        },
        "metadata" => %{
          "source" => "api"
        }
      }

      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :process,
          task_index: 0,
          attempt: 1,
          repo: TestRepo,
          flow_input: input
        )

      assert ctx.flow_input == input
    end
  end

  describe "type constraints" do
    test "run_id can be any string (UUID format)" do
      ctx =
        Context.new(
          run_id: "custom-run-id-123",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx.run_id == "custom-run-id-123"
    end

    test "step_slug must be an atom" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :my_step,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert is_atom(ctx.step_slug)
    end

    test "task_index is a non-negative integer" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert is_integer(ctx.task_index)
      assert ctx.task_index >= 0
    end

    test "attempt is a positive integer" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert is_integer(ctx.attempt)
      assert ctx.attempt > 0
    end

    test "repo is a module atom" do
      ctx =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert is_atom(ctx.repo)
    end

    test "flow_input is either :not_loaded or a map" do
      ctx1 =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo
        )

      assert ctx1.flow_input == :not_loaded

      ctx2 =
        Context.new(
          run_id: "550e8400-e29b-41d4-a716-446655440000",
          step_slug: :test,
          task_index: 0,
          attempt: 1,
          repo: TestRepo,
          flow_input: %{"key" => "value"}
        )

      assert is_map(ctx2.flow_input)
    end
  end
end
