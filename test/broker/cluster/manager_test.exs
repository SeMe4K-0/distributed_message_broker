defmodule Broker.Cluster.ManagerTest do
  use ExUnit.Case, async: false

  alias Broker.Cluster.Manager

  # Manager is registered as a named process, so we can't start a fresh one
  # while the application is running. We use the app-managed Manager directly
  # and clean up any fake nodes we add during each test.

  setup do
    on_exit(fn ->
      # Remove any fake nodes added during the test
      mgr = Process.whereis(Manager)
      if mgr do
        for n <- Manager.nodes(), n != node() do
          Process.send(mgr, {:nodedown, n}, [])
        end
        Process.sleep(20)
      end
    end)

    :ok
  end

  test "single node — all partitions owned by self" do
    assert Manager.node_for("topic", 0) == node()
    assert Manager.node_for("topic", 1) == node()
    assert Manager.node_for("other", 99) == node()
  end

  test "nodes/0 includes current node" do
    assert node() in Manager.nodes()
  end

  test "simulates nodeup — adds node to ring" do
    fake_node = :"fake_up@127.0.0.1"

    Process.send(Process.whereis(Manager), {:nodeup, fake_node}, [])
    Process.sleep(20)

    assert fake_node in Manager.nodes()

    all_local = Enum.all?(0..99, fn i -> Manager.node_for("t", i) == node() end)
    refute all_local, "Expected some keys to route to the fake node"
  end

  test "simulates nodedown — removes node from ring" do
    fake_node = :"fake_down@127.0.0.1"
    mgr = Process.whereis(Manager)

    Process.send(mgr, {:nodeup, fake_node}, [])
    Process.sleep(20)
    assert fake_node in Manager.nodes()

    Process.send(mgr, {:nodedown, fake_node}, [])
    Process.sleep(20)
    refute fake_node in Manager.nodes()

    assert Enum.all?(0..99, fn i -> Manager.node_for("t", i) == node() end)
  end

  test "ring is deterministic — same key maps to same node across calls" do
    fake_node = :"stable@127.0.0.1"
    Process.send(Process.whereis(Manager), {:nodeup, fake_node}, [])
    Process.sleep(20)

    key_owner = Manager.node_for("events", 5)
    assert Manager.node_for("events", 5) == key_owner
    assert Manager.node_for("events", 5) == key_owner
  end
end
