defmodule Broker.Cluster.Manager do
  @moduledoc false
  use GenServer

  require Logger

  alias Broker.Cluster.HashRing

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the node that owns the given (topic, partition)."
  @spec node_for(String.t(), non_neg_integer()) :: node()
  def node_for(topic, partition) do
    GenServer.call(__MODULE__, {:node_for, {topic, partition}})
  end

  @doc "Returns all known nodes (including self)."
  @spec nodes() :: [node()]
  def nodes(), do: GenServer.call(__MODULE__, :nodes)

  @doc "Explicitly connect to a peer and add it to the ring."
  @spec connect_node(node()) :: :ok | {:error, term()}
  def connect_node(peer) do
    GenServer.call(__MODULE__, {:connect, peer})
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    peers = Keyword.get(opts, :peers, Application.get_env(:broker, :peers, []))

    if Node.alive?() do
      :net_kernel.monitor_nodes(true)
    end

    ring =
      Enum.reduce(peers, HashRing.new([node()]), fn peer, acc ->
        if Node.connect(peer) do
          Logger.info("[ClusterManager] Connected to #{peer}")
          HashRing.add_node(acc, peer)
        else
          Logger.warning("[ClusterManager] Could not connect to #{peer}")
          acc
        end
      end)

    Logger.info("[ClusterManager] Started. Nodes: #{inspect(HashRing.nodes(ring))}")
    {:ok, %{ring: ring}}
  end

  @impl GenServer
  def handle_call({:node_for, key}, _from, state) do
    {:reply, HashRing.node_for(state.ring, key), state}
  end

  @impl GenServer
  def handle_call(:nodes, _from, state) do
    {:reply, HashRing.nodes(state.ring), state}
  end

  @impl GenServer
  def handle_call({:connect, peer}, _from, state) do
    if Node.connect(peer) do
      new_ring = HashRing.add_node(state.ring, peer)
      Logger.info("[ClusterManager] Node joined: #{peer}")
      {:reply, :ok, %{state | ring: new_ring}}
    else
      {:reply, {:error, :not_reachable}, state}
    end
  end

  @impl GenServer
  def handle_info({:nodeup, peer}, state) do
    Logger.info("[ClusterManager] nodeup: #{peer}")
    new_ring = HashRing.add_node(state.ring, peer)
    {:noreply, %{state | ring: new_ring}}
  end

  @impl GenServer
  def handle_info({:nodedown, peer}, state) do
    Logger.warning("[ClusterManager] nodedown: #{peer}")
    new_ring = HashRing.remove_node(state.ring, peer)
    {:noreply, %{state | ring: new_ring}}
  end
end
