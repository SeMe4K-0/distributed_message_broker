defmodule Broker.Topic.PartitionSupervisor do
  @moduledoc false
  use Supervisor

  alias Broker.Cluster.Manager
  alias Broker.Log.SegmentManager
  alias Broker.Raft.Server, as: RaftServer

  def start_link({topic, partition, opts}) do
    Supervisor.start_link(__MODULE__, {topic, partition, opts})
  end

  @impl Supervisor
  def init({topic, partition, opts}) do
    data_dir = Keyword.get(opts, :data_dir, Application.get_env(:broker, :data_dir, "data"))
    dir = Path.join([data_dir, topic, "#{partition}"])
    File.mkdir_p!(dir)

    raft_peers = compute_raft_peers(topic, partition)
    apply_fn = make_apply_fn(topic, partition)

    children = [
      {Broker.Log.SegmentManager, {topic, partition, opts}},
      {Broker.Log.Compactor,
       [dir: dir, retention_ms: Keyword.get(opts, :retention_ms, 7 * 24 * 60 * 60 * 1000)]},
      {Broker.Topic.Partition, {topic, partition, opts}},
      Supervisor.child_spec(
        {RaftServer,
         [
           name: RaftServer.name(topic, partition),
           node_id: node(),
           peers: raft_peers,
           apply_fn: apply_fn
         ]},
        id: {:raft, topic, partition}
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp compute_raft_peers(topic, partition) do
    case Process.whereis(Manager) do
      nil ->
        []

      _ ->
        replicas = Manager.replicas_for(topic, partition)
        others = Enum.reject(replicas, &(&1 == node()))
        Enum.map(others, fn n -> {RaftServer.name(topic, partition), n} end)
    end
  end

  # apply_fn is invoked on every replica when an entry is committed.
  # It writes the record to the local SegmentManager.
  defp make_apply_fn(topic, partition) do
    fn
      {:append, key, value} ->
        mgr = Broker.Topic.Partition.manager_via(topic, partition)
        SegmentManager.append(mgr, key, value)
    end
  end
end
