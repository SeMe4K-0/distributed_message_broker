defmodule Broker.Log.SegmentIndexTest do
  use ExUnit.Case, async: true

  alias Broker.Log.SegmentIndex

  describe "binary_search" do
    test "returns :not_found for empty index" do
      assert :not_found = SegmentIndex.binary_search(<<>>, 0)
    end

    test "finds the first entry" do
      data = SegmentIndex.build_entry(0, 100)
      assert {:ok, 100} = SegmentIndex.binary_search(data, 0)
    end

    test "finds the last entry" do
      data =
        SegmentIndex.build_entry(0, 0) <>
          SegmentIndex.build_entry(1, 50) <>
          SegmentIndex.build_entry(2, 120)

      assert {:ok, 120} = SegmentIndex.binary_search(data, 2)
    end

    test "finds an exact match in the middle" do
      data =
        SegmentIndex.build_entry(0, 0) <>
          SegmentIndex.build_entry(10, 400) <>
          SegmentIndex.build_entry(20, 900)

      assert {:ok, 400} = SegmentIndex.binary_search(data, 10)
    end

    test "returns the floor entry when no exact match" do
      data =
        SegmentIndex.build_entry(0, 0) <>
          SegmentIndex.build_entry(10, 400) <>
          SegmentIndex.build_entry(20, 900)

      # 15 is between 10 and 20 — should return position for 10
      assert {:ok, 400} = SegmentIndex.binary_search(data, 15)
    end

    test "returns :not_found when target is before first entry" do
      data = SegmentIndex.build_entry(5, 200)
      assert :not_found = SegmentIndex.binary_search(data, 2)
    end

    test "works with many entries" do
      data =
        Enum.reduce(0..99, <<>>, fn i, acc ->
          acc <> SegmentIndex.build_entry(i, i * 100)
        end)

      assert {:ok, 5000} = SegmentIndex.binary_search(data, 50)
      assert {:ok, 9900} = SegmentIndex.binary_search(data, 99)
      assert {:ok, 0} = SegmentIndex.binary_search(data, 0)
    end
  end

  describe "entry_count" do
    test "empty binary" do
      assert SegmentIndex.entry_count(<<>>) == 0
    end

    test "three entries" do
      data =
        SegmentIndex.build_entry(0, 0) <>
          SegmentIndex.build_entry(1, 50) <>
          SegmentIndex.build_entry(2, 100)

      assert SegmentIndex.entry_count(data) == 3
    end
  end
end
