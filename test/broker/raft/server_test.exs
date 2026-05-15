defmodule Broker.Raft.ServerTest do
  use ExUnit.Case, async: false

  alias Broker.Raft.Server

  # Helper: start a 3-node Raft cluster where each node runs in this BEAM.
  # Each node uses a unique atom name and an Agent to collect applied commands.
  defp start_cluster(n, opts \\ []) do
    test_id = :erlang.unique_integer([:positive])
    names = for i <- 1..n, do: :"raft_test_#{test_id}_#{i}"

    agents =
      for name <- names, into: %{} do
        {:ok, agent} = Agent.start_link(fn -> [] end)
        {name, agent}
      end

    servers =
      for name <- names do
        peers = names -- [name]
        agent = agents[name]
        apply_fn = fn cmd -> Agent.update(agent, &[cmd | &1]) end

        {:ok, pid} =
          Server.start_link(
            [
              name: name,
              node_id: name,
              peers: peers,
              apply_fn: apply_fn
            ] ++ opts
          )

        {name, pid}
      end

    on_exit_cleanup(servers, agents)
    %{servers: Map.new(servers), agents: agents, names: names}
  end

  defp on_exit_cleanup(servers, agents) do
    ExUnit.Callbacks.on_exit(fn ->
      for {_n, pid} <- servers do
        if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1000)
      end

      for {_n, a} <- agents do
        if Process.alive?(a), do: Agent.stop(a)
      end
    end)
  end

  defp wait_for_leader(servers, timeout_ms \\ 3000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Stream.unfold(deadline, fn d ->
      now = System.monotonic_time(:millisecond)

      if now > d do
        nil
      else
        states =
          for {name, pid} <- servers, into: %{}, do: {name, Server.get_state(pid)}

        leaders = Enum.filter(states, fn {_n, s} -> s.role == :leader end)

        case leaders do
          [{leader_name, _state}] ->
            {leader_name, nil}

          _ ->
            Process.sleep(50)
            {nil, d}
        end
      end
    end)
    |> Enum.find(&(&1 != nil)) || flunk("No leader elected within #{timeout_ms}ms")
  end

  describe "leader election" do
    test "exactly one leader is elected in a 3-node cluster" do
      %{servers: servers} = start_cluster(3)
      leader_name = wait_for_leader(servers)

      states = for {n, pid} <- servers, into: %{}, do: {n, Server.get_state(pid)}
      leaders = Enum.filter(states, fn {_n, s} -> s.role == :leader end)
      followers = Enum.filter(states, fn {_n, s} -> s.role == :follower end)

      assert length(leaders) == 1
      assert length(followers) == 2

      [{_, leader_state} | _] = leaders
      assert leader_state.leader == leader_name

      # All followers agree on the leader and term
      for {_n, s} <- followers do
        assert s.leader == leader_name
        assert s.term == leader_state.term
      end
    end

    test "single-node cluster becomes leader immediately" do
      test_id = :erlang.unique_integer([:positive])
      name = :"raft_solo_#{test_id}"
      {:ok, agent} = Agent.start_link(fn -> [] end)

      {:ok, pid} =
        Server.start_link(
          name: name,
          node_id: name,
          peers: [],
          apply_fn: fn cmd -> Agent.update(agent, &[cmd | &1]) end
        )

      Process.sleep(50)
      state = Server.get_state(pid)
      assert state.role == :leader

      # Can propose immediately
      assert {:ok, :ok} = Server.propose(pid, {:write, "x"})
      Process.sleep(50)
      assert Agent.get(agent, & &1) == [{:write, "x"}]

      GenServer.stop(pid)
      Agent.stop(agent)
    end
  end

  describe "log replication" do
    test "leader replicates committed entries to followers" do
      %{servers: servers, agents: agents} = start_cluster(3)
      leader_name = wait_for_leader(servers)
      leader_pid = servers[leader_name]

      # Propose 3 commands
      for i <- 1..3 do
        assert {:ok, :ok} = Server.propose(leader_pid, {:write, "v#{i}"}, 2000)
      end

      Process.sleep(300)

      # All three nodes should have applied all three commands
      for {_name, agent} <- agents do
        applied = Agent.get(agent, & &1) |> Enum.reverse()
        assert applied == [{:write, "v1"}, {:write, "v2"}, {:write, "v3"}]
      end

      # commit_index should be the same on all nodes
      indices =
        for {_n, pid} <- servers, do: Server.get_state(pid).commit_index

      assert Enum.uniq(indices) == [3]
    end

    test "non-leader rejects proposals with leader hint" do
      %{servers: servers} = start_cluster(3)
      leader_name = wait_for_leader(servers)

      [{_follower_name, follower_pid} | _] =
        Enum.filter(servers, fn {n, _p} -> n != leader_name end)

      assert {:error, {:not_leader, hint}} = Server.propose(follower_pid, {:write, "x"}, 1000)
      assert hint == leader_name
    end
  end

  describe "failover" do
    test "new leader is elected after the current leader is killed" do
      # The test process is linked to the started Raft servers. We trap exits
      # so killing the leader doesn't propagate and kill us.
      Process.flag(:trap_exit, true)

      %{servers: servers, agents: _agents} = start_cluster(3)
      leader_name = wait_for_leader(servers)
      leader_pid = servers[leader_name]

      # Kill the leader
      Process.exit(leader_pid, :kill)
      assert_receive {:EXIT, ^leader_pid, :killed}, 1000

      # Wait for a new leader among the remaining two
      remaining = Map.delete(servers, leader_name)
      new_leader_name = wait_for_leader(remaining, 5000)
      assert new_leader_name != leader_name

      # The new leader should still be able to accept proposals
      new_leader_pid = remaining[new_leader_name]
      assert {:ok, :ok} = Server.propose(new_leader_pid, {:write, "after_failover"}, 2000)
    end
  end
end
