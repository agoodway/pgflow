defmodule PgFlow.WorkerOptionsTest do
  use ExUnit.Case, async: true

  alias PgFlow.TestRepo
  alias PgFlow.WorkerOptions

  describe "validate!/1" do
    test "accepts max_concurrency and batch_size" do
      assert WorkerOptions.validate!(max_concurrency: 4, batch_size: 2) ==
               [max_concurrency: 4, batch_size: 2]
    end

    test "rejects unknown keys, zero and non-integers" do
      assert_raise ArgumentError, ~r/:timeout/, fn ->
        WorkerOptions.validate!(timeout: 5)
      end

      assert_raise ArgumentError, ~r/max_concurrency/, fn ->
        WorkerOptions.validate!(max_concurrency: 0)
      end

      assert_raise ArgumentError, fn ->
        WorkerOptions.validate!(batch_size: "4")
      end
    end
  end

  describe "validate_scale!/1" do
    test "requires at least one option" do
      assert_raise ArgumentError, ~r/requires :max_concurrency or :batch_size/, fn ->
        WorkerOptions.validate_scale!([])
      end
    end
  end

  describe "resolve/3" do
    test "applies global, then worker_options, then overrides" do
      global = [
        max_concurrency: 8,
        batch_size: 8,
        worker_options: %{Foo => [max_concurrency: 4]}
      ]

      assert WorkerOptions.resolve(global, Foo, []) ==
               %{max_concurrency: 4, batch_size: 8}

      assert WorkerOptions.resolve(global, Foo, batch_size: 2) ==
               %{max_concurrency: 4, batch_size: 2}

      assert WorkerOptions.resolve(global, Bar, []) ==
               %{max_concurrency: 8, batch_size: 8}

      assert WorkerOptions.resolve([max_concurrency: 8, batch_size: 8], Foo, []) ==
               %{max_concurrency: 8, batch_size: 8}
    end
  end

  describe "split/1" do
    test "separates worker keys from other start options" do
      assert WorkerOptions.split(repo: TestRepo, max_concurrency: 4) ==
               {[max_concurrency: 4], [repo: TestRepo]}
    end
  end
end
