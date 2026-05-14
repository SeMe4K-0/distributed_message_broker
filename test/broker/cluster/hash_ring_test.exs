defmodule Broker.Cluster.HashRingTest do
  use ExUnit.Case, async: true

  alias Broker.Cluster.HashRing

  @n1 :"broker1@127.0.0.1"
  @n2 :"broker2@127.0.0.1"
  @n3 :"broker3@127.0.0.1"

  describe "new/1" do
    test "single node owns all keys" do
      ring = HashRing.new([@n1])
      assert HashRing.node_for(ring, "topic/0") == @n1
      assert HashRing.node_for(ring, "topic/1") == @n1
      assert HashRing.node_for(ring, {:any, :key}) == @n1
    end

    test "empty ring falls back to node()" do
      ring = %{ring: [], nodes: MapSet.new()}
      assert HashRing.node_for(ring, "x") == node()
    end

    test "multiple nodes share keys" do
      ring = HashRing.new([@n1, @n2, @n3])
      keys = for i <- 0..299, do: "topic/#{i}"
      owners = Enum.map(keys, &HashRing.node_for(ring, &1))
      # All three nodes appear
      assert @n1 in owners
      assert @n2 in owners
      assert @n3 in owners
    end
  end

  describe "add_node/2" do
    test "returns the same ring when adding a node that already exists" do
      ring = HashRing.new([@n1, @n2])
      ring2 = HashRing.add_node(ring, @n1)
      assert ring == ring2
    end

    test "new node receives a share of keys" do
      ring2 = HashRing.new([@n1, @n2])
      ring3 = HashRing.add_node(ring2, @n3)

      keys = for i <- 0..599, do: "k#{i}"
      before_owners = Enum.map(keys, &HashRing.node_for(ring2, &1))
      after_owners = Enum.map(keys, &HashRing.node_for(ring3, &1))

      moved_to_n3 = Enum.count(after_owners, &(&1 == @n3))
      # With 3 nodes and 600 keys, n3 should receive roughly 200 keys
      assert moved_to_n3 > 100, "Expected n3 to receive >100 keys, got #{moved_to_n3}"

      # Minimal disruption: keys NOT going to n3 should keep their original owner
      still_same =
        Enum.zip(before_owners, after_owners)
        |> Enum.count(fn {b, a} -> b == a end)

      # At least ~60% of keys should stay put (expected ~2/3, allow variance from vnodes)
      assert still_same >= div(length(keys) * 6, 10)
    end
  end

  describe "remove_node/2" do
    test "removed node no longer appears" do
      ring = HashRing.new([@n1, @n2, @n3])
      ring2 = HashRing.remove_node(ring, @n3)

      keys = for i <- 0..299, do: "k#{i}"
      owners = Enum.map(keys, &HashRing.node_for(ring2, &1))
      assert @n3 not in owners
    end

    test "only keys that were on the removed node get reassigned" do
      ring = HashRing.new([@n1, @n2, @n3])
      ring2 = HashRing.remove_node(ring, @n3)

      keys = for i <- 0..599, do: "k#{i}"

      moved =
        Enum.count(keys, fn k ->
          HashRing.node_for(ring, k) != HashRing.node_for(ring2, k)
        end)

      # Only ~1/3 of keys (those on n3) should move
      assert moved < div(length(keys), 2),
             "Too many keys moved: #{moved} out of #{length(keys)}"
    end
  end

  describe "nodes/1" do
    test "returns all registered nodes" do
      ring = HashRing.new([@n1, @n2])
      assert Enum.sort(HashRing.nodes(ring)) == Enum.sort([@n1, @n2])
    end

    test "updates after add/remove" do
      ring = HashRing.new([@n1]) |> HashRing.add_node(@n2) |> HashRing.remove_node(@n1)
      assert HashRing.nodes(ring) == [@n2]
    end
  end

  describe "determinism" do
    test "same key always maps to same node" do
      ring = HashRing.new([@n1, @n2, @n3])
      key = {"events", 7}
      owner = HashRing.node_for(ring, key)
      assert HashRing.node_for(ring, key) == owner
      assert HashRing.node_for(ring, key) == owner
    end

    test "ring built from same nodes is identical regardless of input order" do
      ring_abc = HashRing.new([@n1, @n2, @n3])
      ring_cba = HashRing.new([@n3, @n2, @n1])
      # ring structures should be identical (sorted)
      assert ring_abc.ring == ring_cba.ring
    end
  end
end
