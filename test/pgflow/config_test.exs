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
          min_poll_interval: 100
        )

      assert config[:repo] == ValidTestRepo
      assert config[:max_concurrency] == 20
      assert config[:min_poll_interval] == 100
    end

    test "accepts all valid options" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          flows: [SomeFlow, AnotherFlow],
          jobs: [SomeJob],
          max_concurrency: 20,
          batch_size: 15,
          signal_strategy: :notify,
          min_poll_interval: 100,
          max_poll_interval: 10_000,
          notify_fallback_interval: 60_000,
          notify_throttle_ms: 500,
          waiting_recovery_batch_size: 25,
          attach_default_logger: false
        )

      assert config[:repo] == ValidTestRepo
      assert config[:flows] == [SomeFlow, AnotherFlow]
      assert config[:jobs] == [SomeJob]
      assert config[:max_concurrency] == 20
      assert config[:batch_size] == 15
      assert config[:signal_strategy] == :notify
      assert config[:min_poll_interval] == 100
      assert config[:max_poll_interval] == 10_000
      assert config[:notify_fallback_interval] == 60_000
      assert config[:notify_throttle_ms] == 500
      assert config[:waiting_recovery_batch_size] == 25
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

    test "applies default for :signal_strategy" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:signal_strategy] == :polling
    end

    test "applies default for :min_poll_interval" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:min_poll_interval] == 1_000
    end

    test "applies default for :max_poll_interval" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:max_poll_interval] == 5_000
    end

    test "applies default for :notify_fallback_interval" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:notify_fallback_interval] == 30_000
    end

    test "applies default for :notify_throttle_ms" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:notify_throttle_ms] == 250
    end

    test "applies default for :attach_default_logger" do
      config = Config.validate!(repo: ValidTestRepo)

      # Default is false since PgFlow.Logger handles structured logging
      assert config[:attach_default_logger] == false
    end

    test "applies default for :jobs" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:jobs] == []
    end

    test "applies all defaults when only repo is provided" do
      config = Config.validate!(repo: ValidTestRepo)

      assert config[:flows] == []
      assert config[:jobs] == []
      assert config[:max_concurrency] == 10
      assert config[:batch_size] == 10
      assert config[:signal_strategy] == :polling
      assert config[:min_poll_interval] == 1_000
      assert config[:max_poll_interval] == 5_000
      assert config[:notify_fallback_interval] == 30_000
      assert config[:notify_throttle_ms] == 250
      assert config[:attach_default_logger] == false
    end

    test "applies default 15_000 for :recovery_interval" do
      config = Config.validate!(repo: ValidTestRepo)
      assert config[:recovery_interval] == 15_000
    end

    test "applies default 15_000 for :waiting_recovery_interval" do
      config = Config.validate!(repo: ValidTestRepo)
      assert config[:waiting_recovery_interval] == 15_000
    end

    test "applies default 100 for :waiting_recovery_batch_size" do
      config = Config.validate!(repo: ValidTestRepo)
      assert config[:waiting_recovery_batch_size] == 100
    end

    test "applies default 60 for :stale_threshold" do
      config = Config.validate!(repo: ValidTestRepo)
      assert config[:stale_threshold] == 60
    end

    test "applies default nil for :worker_name" do
      config = Config.validate!(repo: ValidTestRepo)
      assert config[:worker_name] == nil
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
        Config.validate!(max_concurrency: 20, min_poll_interval: 200)
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

    test "raises when :waiting_recovery_batch_size is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, waiting_recovery_batch_size: 0)
      end
    end

    test "raises when :min_poll_interval is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, min_poll_interval: 0)
      end
    end

    test "raises when :max_poll_interval is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, max_poll_interval: 0)
      end
    end

    test "raises when :notify_fallback_interval is not a positive integer" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, notify_fallback_interval: 0)
      end
    end

    test "raises when :notify_throttle_ms is negative" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, notify_throttle_ms: -1)
      end
    end

    test "raises when :signal_strategy is invalid" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, signal_strategy: :invalid)
      end
    end

    test "raises when :recovery_interval is 0" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, recovery_interval: 0)
      end
    end

    test "raises when :recovery_interval is negative" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, recovery_interval: -1)
      end
    end

    test "raises when :stale_threshold is 0" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, stale_threshold: 0)
      end
    end

    test "raises when :stale_threshold is negative" do
      assert_raise ArgumentError, ~r/invalid PgFlow configuration/, fn ->
        Config.validate!(repo: ValidTestRepo, stale_threshold: -1)
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

    test "schema includes :jobs option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :jobs)
      assert schema[:jobs][:default] == []
      assert schema[:jobs][:type] == {:list, :atom}
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

    test "schema includes :signal_strategy option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :signal_strategy)
      assert schema[:signal_strategy][:default] == :polling
      assert schema[:signal_strategy][:type] == {:in, [:polling, :notify]}
    end

    test "schema includes :min_poll_interval option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :min_poll_interval)
      assert schema[:min_poll_interval][:default] == 1_000
      assert schema[:min_poll_interval][:type] == :pos_integer
    end

    test "schema includes :max_poll_interval option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :max_poll_interval)
      assert schema[:max_poll_interval][:default] == 5_000
      assert schema[:max_poll_interval][:type] == :pos_integer
    end

    test "schema includes :notify_fallback_interval option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :notify_fallback_interval)
      assert schema[:notify_fallback_interval][:default] == 30_000
      assert schema[:notify_fallback_interval][:type] == :pos_integer
    end

    test "schema includes :notify_throttle_ms option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :notify_throttle_ms)
      assert schema[:notify_throttle_ms][:default] == 250
      assert schema[:notify_throttle_ms][:type] == :non_neg_integer
    end

    test "schema includes :attach_default_logger option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :attach_default_logger)
      # Default is false since PgFlow.Logger handles structured logging
      assert schema[:attach_default_logger][:default] == false
      assert schema[:attach_default_logger][:type] == :boolean
    end

    test "schema includes :recovery_interval option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :recovery_interval)
      assert schema[:recovery_interval][:default] == 15_000
      assert schema[:recovery_interval][:type] == :pos_integer
    end

    test "schema includes :stale_threshold option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :stale_threshold)
      assert schema[:stale_threshold][:default] == 60
      assert schema[:stale_threshold][:type] == :pos_integer
    end

    test "schema includes :worker_name option with default" do
      schema = Config.schema()

      assert Keyword.has_key?(schema, :worker_name)
      assert schema[:worker_name][:default] == nil
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

    test "accepts minimum valid values for pos_integer fields and zero for non_neg_integer fields" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          max_concurrency: 1,
          batch_size: 1,
          min_poll_interval: 1,
          max_poll_interval: 1,
          notify_fallback_interval: 1,
          notify_throttle_ms: 0
        )

      assert config[:max_concurrency] == 1
      assert config[:batch_size] == 1
      assert config[:min_poll_interval] == 1
      assert config[:max_poll_interval] == 1
      assert config[:notify_fallback_interval] == 1
      assert config[:notify_throttle_ms] == 0
    end

    test "accepts large positive integer values" do
      config =
        Config.validate!(
          repo: ValidTestRepo,
          max_concurrency: 1000,
          batch_size: 1000,
          min_poll_interval: 10_000,
          max_poll_interval: 60_000,
          notify_fallback_interval: 120_000,
          notify_throttle_ms: 5000
        )

      assert config[:max_concurrency] == 1000
      assert config[:batch_size] == 1000
      assert config[:min_poll_interval] == 10_000
      assert config[:max_poll_interval] == 60_000
      assert config[:notify_fallback_interval] == 120_000
      assert config[:notify_throttle_ms] == 5000
    end

    test "accepts attach_default_logger as false" do
      config = Config.validate!(repo: ValidTestRepo, attach_default_logger: false)

      assert config[:attach_default_logger] == false
    end

    test "accepts attach_default_logger as true" do
      config = Config.validate!(repo: ValidTestRepo, attach_default_logger: true)

      assert config[:attach_default_logger] == true
    end

    test "accepts signal_strategy as :polling" do
      config = Config.validate!(repo: ValidTestRepo, signal_strategy: :polling)

      assert config[:signal_strategy] == :polling
    end

    test "accepts signal_strategy as :notify" do
      config = Config.validate!(repo: ValidTestRepo, signal_strategy: :notify)

      assert config[:signal_strategy] == :notify
    end
  end

  describe "validate!/1 interval bounds validation" do
    test "raises when min_poll_interval > max_poll_interval" do
      assert_raise ArgumentError,
                   ~r/min_poll_interval \(1000ms\) must be <= max_poll_interval \(500ms\)/,
                   fn ->
                     Config.validate!(
                       repo: ValidTestRepo,
                       min_poll_interval: 1000,
                       max_poll_interval: 500
                     )
                   end
    end

    test "accepts when min_poll_interval == max_poll_interval" do
      config =
        Config.validate!(repo: ValidTestRepo, min_poll_interval: 1000, max_poll_interval: 1000)

      assert config[:min_poll_interval] == 1000
      assert config[:max_poll_interval] == 1000
    end

    test "accepts when min_poll_interval < max_poll_interval" do
      config =
        Config.validate!(repo: ValidTestRepo, min_poll_interval: 100, max_poll_interval: 5000)

      assert config[:min_poll_interval] == 100
      assert config[:max_poll_interval] == 5000
    end

    test "raises when max_poll_interval exceeds 5 minute limit" do
      assert_raise ArgumentError,
                   ~r/max_poll_interval \(400000ms\) exceeds maximum allowed \(300000ms = 5 minutes\)/,
                   fn ->
                     Config.validate!(repo: ValidTestRepo, max_poll_interval: 400_000)
                   end
    end

    test "accepts max_poll_interval at exactly 5 minute limit" do
      config = Config.validate!(repo: ValidTestRepo, max_poll_interval: 300_000)

      assert config[:max_poll_interval] == 300_000
    end

    test "raises when notify_fallback_interval exceeds 10 minute limit" do
      assert_raise ArgumentError,
                   ~r/notify_fallback_interval \(700000ms\) exceeds maximum allowed \(600000ms = 10 minutes\)/,
                   fn ->
                     Config.validate!(repo: ValidTestRepo, notify_fallback_interval: 700_000)
                   end
    end

    test "accepts notify_fallback_interval at exactly 10 minute limit" do
      config = Config.validate!(repo: ValidTestRepo, notify_fallback_interval: 600_000)

      assert config[:notify_fallback_interval] == 600_000
    end
  end
end
