defmodule Broker.Log.SegmentTest do
  use ExUnit.Case

  alias Broker.Log.Segment

  setup do
    dir = System.tmp_dir!() |> Path.join("seg_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  defp start_segment(dir, base_offset \\ 0, max_bytes \\ 64 * 1024 * 1024) do
    start_supervised!({Segment, {base_offset, dir, [max_bytes: max_bytes]}})
  end

  describe "append / read_at" do
    test "returns correct offsets starting from base", %{dir: dir} do
      seg = start_segment(dir, 10)
      assert {:ok, 10} = Segment.append(seg, "k", "v0")
      assert {:ok, 11} = Segment.append(seg, "k", "v1")
      assert {:ok, 12} = Segment.append(seg, "k", "v2")
    end

    test "read_at returns the correct entry", %{dir: dir} do
      seg = start_segment(dir)
      Segment.append(seg, "key0", "val0")
      Segment.append(seg, "key1", "val1")
      Segment.append(seg, "key2", "val2")

      assert {:ok, %{offset: 1, key: "key1", value: "val1"}} = Segment.read_at(seg, 1)
    end

    test "read_at returns :not_found for offset before base", %{dir: dir} do
      seg = start_segment(dir, 5)
      Segment.append(seg, "k", "v")
      assert {:error, :not_found} = Segment.read_at(seg, 0)
    end

    test "read_at returns :not_found for offset not yet written", %{dir: dir} do
      seg = start_segment(dir)
      assert {:error, :not_found} = Segment.read_at(seg, 99)
    end
  end

  describe "full?" do
    test "returns false when under limit", %{dir: dir} do
      seg = start_segment(dir, 0, 64 * 1024 * 1024)
      Segment.append(seg, "k", "v")
      refute Segment.full?(seg)
    end

    test "returns true when max_bytes reached", %{dir: dir} do
      seg = start_segment(dir, 0, 1)
      Segment.append(seg, "k", "v")
      assert Segment.full?(seg)
    end
  end

  describe "recovery from disk" do
    test "restores entries and base_offset after restart", %{dir: dir} do
      seg1 = start_segment(dir, 0)
      Segment.append(seg1, "k0", "v0")
      Segment.append(seg1, "k1", "v1")
      stop_supervised!(Segment)

      seg2 = start_segment(dir, 0)
      assert Segment.next_offset(seg2) == 2
      assert {:ok, %{key: "k0"}} = Segment.read_at(seg2, 0)
      assert {:ok, %{key: "k1"}} = Segment.read_at(seg2, 1)
    end
  end
end
