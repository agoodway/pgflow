defmodule PgFlow.Worker.LifecycleTest do
  use ExUnit.Case, async: true

  alias PgFlow.Worker.Lifecycle

  describe "new/0 and current/1" do
    test "creates lifecycle in :created state" do
      lifecycle = Lifecycle.new()
      assert lifecycle.state == :created
    end

    test "current/1 returns the current state" do
      lifecycle = Lifecycle.new()
      assert Lifecycle.current(lifecycle) == :created
    end

    test "returns correct state after transitions" do
      {:ok, lifecycle} = Lifecycle.new() |> Lifecycle.transition(:starting)
      assert Lifecycle.current(lifecycle) == :starting
    end
  end

  describe "transition/2 - valid transitions" do
    test "created -> starting" do
      {:ok, lifecycle} = Lifecycle.new() |> Lifecycle.transition(:starting)
      assert lifecycle.state == :starting
    end

    test "starting -> running" do
      {:ok, lifecycle} =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition(:running)

      assert lifecycle.state == :running
    end

    test "running -> stopping" do
      {:ok, lifecycle} =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition(:stopping)

      assert lifecycle.state == :stopping
    end

    test "stopping -> stopped" do
      {:ok, lifecycle} =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition(:stopped)

      assert lifecycle.state == :stopped
    end

    test "full lifecycle path" do
      lifecycle = Lifecycle.new()
      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :starting)
      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :running)
      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :stopping)
      {:ok, lifecycle} = Lifecycle.transition(lifecycle, :stopped)
      assert lifecycle.state == :stopped
    end
  end

  describe "transition/2 - idempotent (same-state)" do
    test "created -> created returns {:ok, same}" do
      lifecycle = Lifecycle.new()
      {:ok, same} = Lifecycle.transition(lifecycle, :created)
      assert same == lifecycle
    end

    test "running -> running returns {:ok, same}" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)

      {:ok, same} = Lifecycle.transition(lifecycle, :running)
      assert same == lifecycle
    end

    test "stopped -> stopped returns {:ok, same}" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition!(:stopped)

      {:ok, same} = Lifecycle.transition(lifecycle, :stopped)
      assert same == lifecycle
    end
  end

  describe "transition/2 - invalid transitions" do
    test "created -> running returns error" do
      lifecycle = Lifecycle.new()
      assert {:error, {:invalid_transition, :created, :running}} = Lifecycle.transition(lifecycle, :running)
    end

    test "created -> stopped returns error" do
      lifecycle = Lifecycle.new()
      assert {:error, {:invalid_transition, :created, :stopped}} = Lifecycle.transition(lifecycle, :stopped)
    end

    test "running -> created returns error" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)

      assert {:error, {:invalid_transition, :running, :created}} = Lifecycle.transition(lifecycle, :created)
    end

    test "stopped -> running returns error" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition!(:stopped)

      assert {:error, {:invalid_transition, :stopped, :running}} = Lifecycle.transition(lifecycle, :running)
    end

    test "stopped -> starting returns error (cannot leave terminal)" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition!(:stopped)

      assert {:error, {:invalid_transition, :stopped, :starting}} = Lifecycle.transition(lifecycle, :starting)
    end
  end

  describe "transition!/2" do
    test "returns lifecycle on valid transition" do
      lifecycle = Lifecycle.transition!(Lifecycle.new(), :starting)
      assert lifecycle.state == :starting
    end

    test "raises ArgumentError on invalid transition" do
      assert_raise ArgumentError, fn ->
        Lifecycle.transition!(Lifecycle.new(), :running)
      end
    end

    test "error message includes from and to states" do
      assert_raise ArgumentError, ~r/Cannot transition from created to running/, fn ->
        Lifecycle.transition!(Lifecycle.new(), :running)
      end
    end
  end

  describe "state predicates" do
    test "created?/1 is true only in :created" do
      lifecycle = Lifecycle.new()
      assert Lifecycle.created?(lifecycle)
      refute Lifecycle.starting?(lifecycle)
      refute Lifecycle.running?(lifecycle)
      refute Lifecycle.stopping?(lifecycle)
      refute Lifecycle.stopped?(lifecycle)
    end

    test "starting?/1 is true only in :starting" do
      lifecycle = Lifecycle.transition!(Lifecycle.new(), :starting)
      refute Lifecycle.created?(lifecycle)
      assert Lifecycle.starting?(lifecycle)
      refute Lifecycle.running?(lifecycle)
    end

    test "running?/1 is true only in :running" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)

      refute Lifecycle.created?(lifecycle)
      refute Lifecycle.starting?(lifecycle)
      assert Lifecycle.running?(lifecycle)
      refute Lifecycle.stopping?(lifecycle)
      refute Lifecycle.stopped?(lifecycle)
    end

    test "stopping?/1 is true only in :stopping" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)

      refute Lifecycle.running?(lifecycle)
      assert Lifecycle.stopping?(lifecycle)
      refute Lifecycle.stopped?(lifecycle)
    end

    test "stopped?/1 is true only in :stopped" do
      lifecycle =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition!(:stopped)

      refute Lifecycle.stopping?(lifecycle)
      assert Lifecycle.stopped?(lifecycle)
    end
  end

  describe "can_accept_work?/1" do
    test "true only in :running" do
      running =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)

      assert Lifecycle.can_accept_work?(running)
    end

    test "false in :created" do
      refute Lifecycle.can_accept_work?(Lifecycle.new())
    end

    test "false in :starting" do
      starting = Lifecycle.transition!(Lifecycle.new(), :starting)
      refute Lifecycle.can_accept_work?(starting)
    end

    test "false in :stopping" do
      stopping =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)

      refute Lifecycle.can_accept_work?(stopping)
    end

    test "false in :stopped" do
      stopped =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition!(:stopped)

      refute Lifecycle.can_accept_work?(stopped)
    end
  end

  describe "terminal?/1" do
    test "true only in :stopped" do
      stopped =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)
        |> Lifecycle.transition!(:stopped)

      assert Lifecycle.terminal?(stopped)
    end

    test "false in :created" do
      refute Lifecycle.terminal?(Lifecycle.new())
    end

    test "false in :running" do
      running =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)

      refute Lifecycle.terminal?(running)
    end

    test "false in :stopping" do
      stopping =
        Lifecycle.new()
        |> Lifecycle.transition!(:starting)
        |> Lifecycle.transition!(:running)
        |> Lifecycle.transition!(:stopping)

      refute Lifecycle.terminal?(stopping)
    end
  end
end
