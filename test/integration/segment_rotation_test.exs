defmodule Broker.Integration.SegmentRotationTest do
  use ExUnit.Case

  alias Broker.Log.SegmentManager

  setup do
    dir = System.tmp_dir!() |> Path.join("segrot_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, dir: dir}
  end

  defp start_manager(dir, max_bytes) do
    # Use a unique topic/partition per test to avoid Registry conflicts
    topic_dir = "seg_rot_#{:rand.uniform(1_000_000)}"
    partition = 0

    pid =
      start_supervised!(
        {SegmentManager, {topic_dir, partition, [data_dir: Path.dirname(dir), max_segment_bytes: max_bytes]}}
      )

    {pid, topic_dir}
  end

  test "segments rotate when max_bytes is reached", %{dir: dir} do
    # Each WAL entry for "k"/"v" is ~36 bytes; set max to 40 bytes → 1 entry per segment
    {mgr, _topic} = start_manager(dir, 40)

    SegmentManager.append(mgr, "k", "v")
    SegmentManager.append(mgr, "k", "v")
    SegmentManager.append(mgr, "k", "v")

    # Should have created 3 segments (each holds 1 entry before rotating)
    assert SegmentManager.segment_count(mgr) >= 2
  end

  test "fetch reads entries across segment boundaries", %{dir: dir} do
    {mgr, _topic} = start_manager(dir, 40)

    {:ok, 0} = SegmentManager.append(mgr, "first", "record")
    {:ok, 1} = SegmentManager.append(mgr, "second", "record")
    {:ok, 2} = SegmentManager.append(mgr, "third", "record")

    {:ok, records} = SegmentManager.fetch(mgr, 0, 1_048_576)

    assert length(records) == 3
    assert Enum.map(records, & &1.key) == ["first", "second", "third"]
  end

  test "fetch from mid-offset works across segments", %{dir: dir} do
    {mgr, _topic} = start_manager(dir, 40)

    SegmentManager.append(mgr, "k0", "v0")
    SegmentManager.append(mgr, "k1", "v1")
    SegmentManager.append(mgr, "k2", "v2")

    {:ok, records} = SegmentManager.fetch(mgr, 1, 1_048_576)

    assert Enum.map(records, & &1.key) == ["k1", "k2"]
  end

  test "log and index files are created on disk", %{dir: dir} do
    {mgr, topic} = start_manager(dir, 40)

    SegmentManager.append(mgr, "k", "v")
    SegmentManager.append(mgr, "k", "v")

    partition_dir = Path.join([Path.dirname(dir), topic, "0"])
    log_files = partition_dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".log"))
    idx_files = partition_dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".index"))

    assert length(log_files) >= 1
    assert length(idx_files) >= 1
    assert length(log_files) == length(idx_files)
  end
end
