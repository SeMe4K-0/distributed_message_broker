defmodule Broker.Topic.PartitionTest do
  use ExUnit.Case

  alias Broker.Topic.Partition

  setup do
    dir = System.tmp_dir!() |> Path.join("partition_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, data_dir: dir}
  end

  defp start_partition(topic, partition, data_dir) do
    start_supervised!({Broker.Topic.PartitionSupervisor, {topic, partition, [data_dir: data_dir]}})
    Partition.via(topic, partition)
  end

  describe "append / fetch" do
    test "returns monotonically increasing offsets", %{data_dir: dir} do
      pid = start_partition("offsets", 0, dir)

      assert {:ok, 0} = Partition.append(pid, "k", "v0")
      assert {:ok, 1} = Partition.append(pid, "k", "v1")
      assert {:ok, 2} = Partition.append(pid, "k", "v2")
    end

    test "fetch from offset 0 returns all records", %{data_dir: dir} do
      pid = start_partition("all", 0, dir)

      Partition.append(pid, "k0", "v0")
      Partition.append(pid, "k1", "v1")
      Partition.append(pid, "k2", "v2")

      {:ok, records} = Partition.fetch(pid, 0, 1_048_576)
      assert length(records) == 3
      assert Enum.map(records, & &1.key) == ["k0", "k1", "k2"]
    end

    test "fetch from mid-offset returns only subsequent records", %{data_dir: dir} do
      pid = start_partition("mid", 0, dir)

      Partition.append(pid, "k0", "v0")
      Partition.append(pid, "k1", "v1")
      Partition.append(pid, "k2", "v2")

      {:ok, records} = Partition.fetch(pid, 1, 1_048_576)
      assert Enum.map(records, & &1.offset) == [1, 2]
    end

    test "fetch from offset beyond hwm returns empty list", %{data_dir: dir} do
      pid = start_partition("empty", 0, dir)

      Partition.append(pid, "k", "v")

      {:ok, records} = Partition.fetch(pid, 99, 1_048_576)
      assert records == []
    end
  end

  describe "high_watermark" do
    test "is 0 on empty partition", %{data_dir: dir} do
      pid = start_partition("hwm", 0, dir)
      assert Partition.high_watermark(pid) == 0
    end

    test "advances after appends", %{data_dir: dir} do
      pid = start_partition("hwm2", 0, dir)

      Partition.append(pid, "k", "v")
      Partition.append(pid, "k", "v")

      assert Partition.high_watermark(pid) == 2
    end
  end

  describe "recovery from disk" do
    test "restores entries and next_offset after restart", %{data_dir: dir} do
      pid1 = start_partition("recover", 0, dir)

      Partition.append(pid1, "k0", "v0")
      Partition.append(pid1, "k1", "v1")

      stop_supervised!(Broker.Topic.PartitionSupervisor)

      pid2 = start_partition("recover", 0, dir)

      assert Partition.high_watermark(pid2) == 2
      {:ok, records} = Partition.fetch(pid2, 0, 1_048_576)
      assert Enum.map(records, & &1.key) == ["k0", "k1"]
    end
  end
end
