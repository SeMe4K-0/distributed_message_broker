defmodule Broker.Cluster.HashRing do
  @moduledoc false

  # Number of virtual nodes per physical node.
  # 150 gives a ~±5% load imbalance with 10 nodes.
  @vnodes 150

  @type t :: %{ring: [{non_neg_integer(), node()}], nodes: MapSet.t(node())}

  @spec new([node()]) :: t()
  def new(nodes \\ [node()]) do
    %{ring: build_ring(nodes), nodes: MapSet.new(nodes)}
  end

  @spec add_node(t(), node()) :: t()
  def add_node(%{ring: ring, nodes: nodes} = state, new_node) do
    if MapSet.member?(nodes, new_node) do
      state
    else
      new_ring = Enum.sort(ring ++ vnode_entries(new_node))
      %{ring: new_ring, nodes: MapSet.put(nodes, new_node)}
    end
  end

  @spec remove_node(t(), node()) :: t()
  def remove_node(%{ring: ring, nodes: nodes}, old_node) do
    new_ring = Enum.reject(ring, fn {_, n} -> n == old_node end)
    %{ring: new_ring, nodes: MapSet.delete(nodes, old_node)}
  end

  # Returns the node responsible for the given key.
  # Falls back to node() when the ring is empty.
  @spec node_for(t(), term()) :: node()
  def node_for(%{ring: []}, _key), do: node()

  def node_for(%{ring: ring}, key) do
    hash = :erlang.phash2(key, 0x7FFFFFFF)

    case Enum.find(ring, fn {h, _} -> h >= hash end) do
      nil -> elem(hd(ring), 1)
      {_, n} -> n
    end
  end

  @spec nodes(t()) :: [node()]
  def nodes(%{nodes: ns}), do: MapSet.to_list(ns)

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp build_ring(nodes) do
    nodes
    |> Enum.flat_map(&vnode_entries/1)
    |> Enum.sort()
  end

  defp vnode_entries(n) do
    for i <- 0..(@vnodes - 1) do
      {:erlang.phash2({n, i}, 0x7FFFFFFF), n}
    end
  end
end
