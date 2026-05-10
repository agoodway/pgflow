defmodule PgFlow.LoggerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  require Logger

  alias PgFlow.Logger, as: PgLogger

  setup do
    previous_format = Application.get_env(:pgflow, :log_format)
    previous_empty = Application.get_env(:pgflow, :log_empty_polls)
    previous_level = Logger.level()

    Application.put_env(:pgflow, :log_format, :simple)
    Logger.configure(level: :debug)

    on_exit(fn ->
      restore_env(:log_format, previous_format)
      restore_env(:log_empty_polls, previous_empty)
      Logger.configure(level: previous_level)
    end)

    :ok
  end

  describe "polling/1 with :log_empty_polls flag" do
    test "is silent by default" do
      Application.delete_env(:pgflow, :log_empty_polls)

      log = capture_log([level: :debug], fn -> PgLogger.polling("test-worker") end)

      refute log =~ "test-worker"
      refute log =~ "polling"
    end

    test "logs when :log_empty_polls is true" do
      Application.put_env(:pgflow, :log_empty_polls, true)

      log = capture_log([level: :debug], fn -> PgLogger.polling("test-worker") end)

      assert log =~ "worker=test-worker"
      assert log =~ "status=polling"
    end
  end

  describe "task_count/2 with :log_empty_polls flag" do
    test "zero count is silent by default" do
      Application.delete_env(:pgflow, :log_empty_polls)

      log = capture_log([level: :debug], fn -> PgLogger.task_count("test-worker", 0) end)

      refute log =~ "no_tasks"
    end

    test "zero count logs when :log_empty_polls is true" do
      Application.put_env(:pgflow, :log_empty_polls, true)

      log = capture_log([level: :debug], fn -> PgLogger.task_count("test-worker", 0) end)

      assert log =~ "worker=test-worker"
      assert log =~ "status=no_tasks"
    end

    test "positive counts always log regardless of :log_empty_polls" do
      Application.delete_env(:pgflow, :log_empty_polls)

      log = capture_log([level: :debug], fn -> PgLogger.task_count("test-worker", 3) end)

      assert log =~ "worker=test-worker"
      assert log =~ "status=starting"
      assert log =~ "task_count=3"
    end
  end

  defp restore_env(_key, nil), do: :ok
  defp restore_env(key, value), do: Application.put_env(:pgflow, key, value)
end
