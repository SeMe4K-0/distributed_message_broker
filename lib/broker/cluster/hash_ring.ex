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

  # Returns up to `n` distinct replica nodes for the given key.
  # The first replica is the primary (from node_for/2); the rest are the next
  # distinct nodes walking the ring clockwise.
  @spec replicas_for(t(), term(), pos_integer()) :: [node()]
  def replicas_for(%{nodes: nodes_set} = state, key, n) do
    all = MapSet.to_list(nodes_set)

    cond do
      all == [] -> [node()]
      n >= length(all) -> all
      true -> walk_unique_nodes(state.ring, key, n)
    end
  end

  # Walks the ring from the hash of `key` clockwise, accumulating the first
  # n distinct nodes encountered (wraps around).
  defp walk_unique_nodes(ring, key, n) do
    hash = :erlang.phash2(key, 0x7FFFFFFF)
    {head, tail} = Enum.split_with(ring, fn {h, _} -> h >= hash end)
    ordered = head ++ tail
    do_walk(ordered, n, [], MapSet.new())
  end

  defp do_walk(_, n, acc, _seen) when length(acc) == n, do: Enum.reverse(acc)
  defp do_walk([], _n, acc, _seen), do: Enum.reverse(acc)

  defp do_walk([{_, node} | rest], n, acc, seen) do
    if MapSet.member?(seen, node) do
      do_walk(rest, n, acc, seen)
    else
      do_walk(rest, n, [node | acc], MapSet.put(seen, node))
    end
  end

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
