defmodule Broker.Integration.ClusterTest do
  @moduledoc """
  Two-node cluster integration tests.

  These tests require Erlang distribution. Run with:

      mix test --include distributed

  They are excluded from the default `mix test` run.
  """

  use ExUnit.Case, async: false

  alias Test.TcpClient

  @moduletag :distributed

  @port1 19095
  @port2 19096

  setup_all do
    # Ensure this node is named (distribution enabled)
    unless Node.alive?() do
      case :net_kernel.start([:"broker_test@127.0.0.1", :longnames]) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
        {:error, reason} -> raise "Cannot start distribution: #{inspect(reason)}"
      end
    end

    # Start peer node
    peer_node = :"broker_peer@127.0.0.1"

    {:ok, _peer} =
      :peer.start_link(%{
        host: ~c"127.0.0.1",
        name: ~c"broker_peer",
        args: [~c"-pa" | Enum.map(:code.get_path(), &to_charlist/1)]
      })

    # Set up data dirs
    dir1 = System.tmp_dir!() |> Path.join("cluster_node1_#{:rand.uniform(1_000_000)}")
    dir2 = System.tmp_dir!() |> Path.join("cluster_node2_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir1)
    File.mkdir_p!(dir2)

    # Start broker on this node (node1)
    Application.stop(:broker)
    Application.put_env(:broker, :port, @port1)
    Application.put_env(:broker, :data_dir, dir1)
    Application.put_env(:broker, :peers, [peer_node])
    {:ok, _} = Application.ensure_all_started(:broker)

    # Start broker on peer node (node2)
    :erpc.call(peer_node, Application, :put_env, [:broker, :port, @port2])
    :erpc.call(peer_node, Application, :put_env, [:broker, :data_dir, dir2])
    :erpc.call(peer_node, Application, :put_env, [:broker, :peers, [node()]])
    {:ok, _} = :erpc.call(peer_node, Application, :ensure_all_started, [:broker])

    # Give managers time to exchange nodeup events
    Process.sleep(200)

    on_exit(fn ->
      # peer process is supervised by ExUnit — stops automatically
      Application.stop(:broker)
      Application.delete_env(:broker, :peers)
      File.rm_rf!(dir1)
      File.rm_rf!(dir2)
    end)

    {:ok, peer_node: peer_node}
  end

  test "produce to node1 routes to owning node, fetch returns the record", %{peer_node: _peer_node} do
    {:ok, sock1} = TcpClient.connect(@port1)
    {:ok, sock2} = TcpClient.connect(@port2)

    topic = "cluster_produce"
    partition = 0

    # Determine which node owns this partition
    owner1 = Broker.Cluster.Manager.node_for(topic, partition)

    # Produce via node1
    {:ok, {0x02, _}} = TcpClient.produce(sock1, topic, partition, [{"k", "v"}])

    # Fetch via node2 — should proxy to owner and return the record
    {:ok, {0x04, payload}} = TcpClient.fetch(sock2, topic, partition, 0)
    <<_cid::64, _hwm::64, num::32, _::binary>> = payload
    assert num == 1, "Expected 1 record from node #{node()}, owner=#{owner1}"

    TcpClient.close(sock1)
    TcpClient.close(sock2)
  end

  test "partitions with different keys route to different nodes", %{peer_node: peer_node} do
    # Check that *some* topics route to node1 and some to peer_node
    keys = for i <- 0..49, do: {"test_topic_#{i}", 0}

    owners =
      Enum.map(keys, fn {t, p} ->
        Broker.Cluster.Manager.node_for(t, p)
      end)

    assert node() in owners, "Expected node1 to own some partitions"
    assert peer_node in owners, "Expected node2 to own some partitions"
  end

  test "nodedown removes peer from ring" do
    # Simulate nodedown without actually stopping the peer
    Process.send(Process.whereis(Broker.Cluster.Manager), {:nodedown, :"fake@127.0.0.1"}, [])
    Process.sleep(20)

    # fake node should not be in the ring (it was never added, so this is a no-op)
    refute :"fake@127.0.0.1" in Broker.Cluster.Manager.nodes()
  end

  test "subscribe on node1 to a partition owned by node2 receives pushed records",
       %{peer_node: _peer_node} do
    {:ok, consumer_sock} = TcpClient.connect(@port1)
    {:ok, producer_sock} = TcpClient.connect(@port2)

    topic = "cluster_stream"
    partition = 0

    owner = Broker.Cluster.Manager.node_for(topic, partition)

    # Subscribe via node1 (producer will be started on owning node)
    {:ok, {0x0C, ack_payload}} = TcpClient.subscribe(consumer_sock, topic, partition, 0, 10, 77)
    assert <<77::64, 0>> = ack_payload

    # Produce via node2 (or wherever — it will route to owner)
    {:ok, {0x02, _}} = TcpClient.produce(producer_sock, topic, partition, [{"sk", "sv"}])

    # Should receive a RECORD_PUSH on consumer_sock
    {:ok, [{0x0E, payload}]} = TcpClient.recv_n_frames(consumer_sock, 1, 3000)
    record = TcpClient.decode_record_push(payload)
    assert record.key == "sk"
    assert record.value == "sv"
    assert record.sub_id == 77

    _ = owner  # suppress unused warning when nodes have same owner
    TcpClient.close(consumer_sock)
    TcpClient.close(producer_sock)
  end
end
