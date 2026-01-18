defmodule PgFlow.ConfigTest do
  use ExUnit.Case, async: true

  alias PgFlow.Config

  # Create a test repo module for validation
  defmodule ValidTestRepo do
    def __adapter__, do: Ecto.Adapters.Postgres
  end

  describe "validate!/1 with valid config" do
    test "accepts config with only required :repo option" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:repo] == ValidTestRepo
    end

    test "returns validated config as keyword list" do
      config = Config.validate!(repo: ValidTestRepo)

      assert is_list(config)
      assert Keyword.keyword?(config)
    end

    test "preserves provided options" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          max_concurrency: 20,
          poll_interval: 200
        )

      assert config[:repo] == ValidTestRepo
      assert config[:max_concurrency] == 20
      assert config[:poll_interval] == 200
    end

    test "accepts all valid options" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          flows: [SomeFlow, AnotherFlow],
          max_concurrency: 20,
          batch_size: 15,
          poll_interval: 200,
          visibility_timeout: 5,
          attach_default_logger: false
        )

      assert config[:repo] == ValidTestRepo
      assert config[:flows] == [SomeFlow, AnotherFlow]
      assert config[:max_concurrency] == 20
      assert config[:batch_size] == 15
      assert config[:poll_interval] == 200
      assert config[:visibility_timeout] == 5
      assert config[:attach_default_logger] == false
    end
  end

  describe "validate!/1 default values" do
    test "applies default for :flows" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:flows] == []
    end

    test "applies default for :max_concurrency" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:max_concurrency] == 10
    end

    test "applies default for :batch_size" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:batch_size] == 10
    end

    test "applies default for :poll_interval" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:poll_interval] == 100
    end

    test "applies default for :visibility_timeout" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:visibility_timeout] == 2
    end

    test "applies default for :attach_default_logger" do
      config = Config.validate!(repo: ValidTestRepo)

      # Default is false since PgFlow.Logger handles structured logging
      assert config[:attach_default_logger] == false
    end

    test "applies all defaults when only repo is provided" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:flows] == []
      assert config[:max_concurrency] == 10
      assert config[:batch_size] == 10
      assert config[:poll_interval] == 100
      assert config[:visibility_timeout] == 2
      assert config[:attach_default_logger] == false
    end
  end

  describe "validate!/1 raises on missing :repo" do
    test "raises ArgumentError when :repo is missing" do
      assert_raise ArgumentError, ~r/required :repo option not found/, fn ->
        Config.validate!([])
      end
    end

    test "raises ArgumentError with empty keyword list" do
      assert_raise ArgumentError, ~r/required :repo option not found/, fn ->
        Config.validate!([])
      end
    end

    test "raises ArgumentError even with other valid options" do
      assert_raise ArgumentError, ~r/required :repo option not found/, fn ->
        Config.validate!(max_concurrency: 20, poll_interval: 200)
      end
    end
  end

  describe "validate!/1 raises on invalid option types" do
    test "raises when :repo is not an atom" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: "not_an_atom")
      end
    end

    test "raises when :flows is not a list" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, flows: :not_a_list)
      end
    end

    test "raises when :flows contains non-atoms" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, flows: ["not_an_atom"])
      end
    end

    test "raises when :max_concurrency is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, max_concurrency: 0)
      end
    end

    test "raises when :max_concurrency is negative" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, max_concurrency: -1)
      end
    end

    test "raises when :batch_size is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, batch_size: 0)
      end
    end

    test "raises when :poll_interval is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, poll_interval: 0)
      end
    end

    test "raises when :visibility_timeout is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, visibility_timeout: 0)
      end
    end

    test "raises when :attach_default_logger is not a boolean" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, attach_default_logger: "true")
      end
    end
  end

  describe "validate!/1 raises on invalid repo module" do
    test "raises when repo module is not loaded" do
      assert_raise ArgumentError, ~r/repo module .* is not loaded/, fn ->
        Config.validate!(repo: NonExistentModule)
      end
    end

    test "raises when repo module does not implement Ecto.Repo" do
      defmodule NotARepo do
        def some_function, do: :ok
      end

      assert_raise ArgumentError, ~r/does not implement Ecto.Repo behaviour/, fn ->
        Config.validate!(repo: NotARepo)
      end
    end
  end

  describe "validate!/1 with unknown options" do
    test "raises on unknown options" do
      assert_raise ArgumentError, ~r/unknown options/, fn ->
        Config.validate!(
          repo: ValidTestRepo,
          unknown_option: :some_value
        )
      end
    end
  end

  describe "schema/0" do
    test "returns the NimbleOptions schema" do
      schema = Config.schema()

      assert is_list(schema)
      assert Keyword.keyword?(schema)
    end

    test "schema includes :repo option" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :repo)
      assert schema[:repo][:required] == true
      assert schema[:repo][:type] == :atom
    end

    test "schema includes :flows option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :flows)
      assert schema[:flows][:default] == []
      assert schema[:flows][:type] == {:list, :atom}
    end

    test "schema includes :max_concurrency option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :max_concurrency)
      assert schema[:max_concurrency][:default] == 10
      assert schema[:max_concurrency][:type] == :pos_integer
    end

    test "schema includes :batch_size option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :batch_size)
      assert schema[:batch_size][:default] == 10
      assert schema[:batch_size][:type] == :pos_integer
    end

    test "schema includes :poll_interval option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :poll_interval)
      assert schema[:poll_interval][:default] == 100
      assert schema[:poll_interval][:type] == :pos_integer
    end

    test "schema includes :visibility_timeout option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :visibility_timeout)
      assert schema[:visibility_timeout][:default] == 2
      assert schema[:visibility_timeout][:type] == :pos_integer
    end

    test "schema includes :attach_default_logger option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :attach_default_logger)
      # Default is false since PgFlow.Logger handles structured logging
      assert schema[:attach_default_logger][:default] == false
      assert schema[:attach_default_logger][:type] == :boolean
    end
  end

  describe "config edge cases" do
    test "accepts flows as empty list" do
      config = Config.validate!(repo: ValidTestRepo, flows: [])

      assert config[:flows] == []
    end

    test "accepts flows with single module" do
      config = Config.validate!(repo: ValidTestRepo, flows: [SomeFlow])

      assert config[:flows] == [SomeFlow]
    end

    test "accepts flows with multiple modules" do
      config = Config.validate!(repo: ValidTestRepo, flows: [Flow1, Flow2, Flow3])

      assert config[:flows] == [Flow1, Flow2, Flow3]
    end

    test "accepts minimum positive integer values" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          max_concurrency: 1,
          batch_size: 1,
          poll_interval: 1,
          visibility_timeout: 1
        )

      assert config[:max_concurrency] == 1
      assert config[:batch_size] == 1
      assert config[:poll_interval] == 1
      assert config[:visibility_timeout] == 1
    end

    test "accepts large positive integer values" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          max_concurrency: 1000,
          batch_size: 1000,
          poll_interval: 10_000,
          visibility_timeout: 3600
        )

      assert config[:max_concurrency] == 1000
      assert config[:batch_size] == 1000
      assert config[:poll_interval] == 10_000
      assert config[:visibility_timeout] == 3600
    end

    test "accepts attach_default_logger as false" do
      config = Config.validate!(repo: ValidTestRepo, attach_default_logger: false)

      assert config[:attach_default_logger] == false
    end

    test "accepts attach_default_logger as true" do
      config = Config.validate!(repo: ValidTestRepo, attach_default_logger: true)

      assert config[:attach_default_logger] == true
    end
  end
end
