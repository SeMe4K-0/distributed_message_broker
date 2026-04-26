defmodule Broker.Log.WALTest do
  use ExUnit.Case, async: true

  alias Broker.Log.WAL

  setup do
    dir = System.tmp_dir!() |> Path.join("wal_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "test.log")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, path: path}
  end

  describe "encode_entry / decode_entry round-trip" do
    test "single entry" do
      bin = WAL.encode_entry(0, "key", "value")
      assert {:ok, entry, <<>>} = WAL.decode_entry(bin)
      assert entry.offset == 0
      assert entry.key == "key"
      assert entry.value == "value"
    end

    test "multiple entries concatenated" do
      data =
        WAL.encode_entry(0, "k0", "v0") <>
          WAL.encode_entry(1, "k1", "v1") <>
          WAL.encode_entry(2, "k2", "v2")

      assert {:ok, entries} = WAL.decode_all(data)
      assert length(entries) == 3
      assert Enum.map(entries, & &1.offset) == [0, 1, 2]
    end

    test "returns :incomplete for partial data" do
      bin = WAL.encode_entry(0, "key", "value")
      partial = binary_part(bin, 0, byte_size(bin) - 2)
      assert {:error, :incomplete} = WAL.decode_entry(partial)
    end

    test "returns :checksum_mismatch for corrupted data" do
      bin = WAL.encode_entry(0, "key", "value")
      # Flip all bits of the middle byte — preserves structure, breaks CRC
      mid = div(byte_size(bin), 2)
      <<before::binary-size(mid), byte, rest::binary>> = bin
      corrupted = <<before::binary, Bitwise.bxor(byte, 0xFF), rest::binary>>
      assert {:error, :checksum_mismatch} = WAL.decode_entry(corrupted)
    end
  end

  describe "append_to_file / read_all" do
    test "persists and recovers entries", %{path: path} do
      :ok = WAL.append_to_file(path, WAL.encode_entry(0, "k0", "v0"))
      :ok = WAL.append_to_file(path, WAL.encode_entry(1, "k1", "v1"))

      assert {:ok, entries} = WAL.read_all(path)
      assert length(entries) == 2
      assert hd(entries).key == "k0"
      assert List.last(entries).key == "k1"
    end

    test "returns empty list for non-existent file", %{path: path} do
      assert {:ok, []} = WAL.read_all(path)
    end
  end
end
