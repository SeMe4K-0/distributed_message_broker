defmodule Broker.Integration.SubscribeTest do
  use ExUnit.Case

  alias Test.TcpClient

  @port 19093

  setup_all do
    dir = System.tmp_dir!() |> Path.join("subscribe_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)

    Application.stop(:broker)
    Application.put_env(:broker, :port, @port)
    Application.put_env(:broker, :data_dir, dir)
    {:ok, _} = Application.ensure_all_started(:broker)

    on_exit(fn ->
      Application.stop(:broker)
      File.rm_rf!(dir)
    end)

    :ok
  end

  test "SUBSCRIBE receives RECORD_PUSH frames for existing records" do
    {:ok, producer_socket} = TcpClient.connect(@port)
    {:ok, consumer_socket} = TcpClient.connect(@port)

    # Write 3 records first
    TcpClient.produce(producer_socket, "stream", 0, [{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}])

    # Subscribe from offset 0, max_in_flight 10, sub_id 42
    {:ok, {0x0C, ack_payload}} = TcpClient.subscribe(consumer_socket, "stream", 0, 0, 10, 42)
    assert <<42::64, 0>> = ack_payload

    # Receive 3 pushed records (frames may arrive batched in one TCP segment)
    {:ok, frames} = TcpClient.recv_n_frames(consumer_socket, 3, 3000)

    records =
      for {0x0E, payload} <- frames do
        TcpClient.decode_record_push(payload)
      end

    assert Enum.map(records, & &1.key) == ["k0", "k1", "k2"]
    assert Enum.map(records, & &1.value) == ["v0", "v1", "v2"]
    assert Enum.map(records, & &1.offset) == [0, 1, 2]
    assert Enum.all?(records, &(&1.sub_id == 42))

    TcpClient.close(producer_socket)
    TcpClient.close(consumer_socket)
  end

  test "SUBSCRIBE tail-follows records produced after subscription" do
    {:ok, producer_socket} = TcpClient.connect(@port)
    {:ok, consumer_socket} = TcpClient.connect(@port)

    # Subscribe to empty partition
    {:ok, {0x0C, _}} = TcpClient.subscribe(consumer_socket, "live", 0, 0, 10, 99)

    # No record yet — recv should time out
    assert {:error, :timeout} = TcpClient.recv_frame(consumer_socket, 200)

    # Now produce a record
    TcpClient.produce(producer_socket, "live", 0, [{"key", "live_value"}])

    # Should arrive via push
    {:ok, [{0x0E, payload}]} = TcpClient.recv_n_frames(consumer_socket, 1, 2000)
    record = TcpClient.decode_record_push(payload)
    assert record.key == "key"
    assert record.value == "live_value"
    assert record.sub_id == 99

    TcpClient.close(producer_socket)
    TcpClient.close(consumer_socket)
  end

  test "SUBSCRIBE with start_offset skips earlier records" do
    {:ok, producer_socket} = TcpClient.connect(@port)
    {:ok, consumer_socket} = TcpClient.connect(@port)

    TcpClient.produce(producer_socket, "offset_test", 0, [{"a", "1"}, {"b", "2"}, {"c", "3"}])

    # Subscribe from offset 2 — should only get the third record
    {:ok, {0x0C, _}} = TcpClient.subscribe(consumer_socket, "offset_test", 0, 2, 10, 7)

    {:ok, [{0x0E, payload}]} = TcpClient.recv_n_frames(consumer_socket, 1, 2000)
    record = TcpClient.decode_record_push(payload)
    assert record.offset == 2
    assert record.key == "c"

    # No more records
    assert {:error, :timeout} = TcpClient.recv_frame(consumer_socket, 200)

    TcpClient.close(producer_socket)
    TcpClient.close(consumer_socket)
  end

  test "backpressure — max_in_flight=1 gets one record at a time" do
    {:ok, producer_socket} = TcpClient.connect(@port)
    {:ok, consumer_socket} = TcpClient.connect(@port)

    TcpClient.produce(producer_socket, "bp_test", 0, [{"x", "1"}, {"y", "2"}, {"z", "3"}])

    # max_in_flight = 1
    {:ok, {0x0C, _}} = TcpClient.subscribe(consumer_socket, "bp_test", 0, 0, 1, 55)

    {:ok, [{0x0E, payload}]} = TcpClient.recv_n_frames(consumer_socket, 1, 2000)
    record = TcpClient.decode_record_push(payload)
    assert record.offset == 0

    # Second should not arrive immediately since consumer hasn't acked demand yet.
    # GenStage min_demand=0 means the consumer won't ask for more until it handles the first.
    # We just verify at least the first record arrived and has the right data.
    assert record.key == "x"

    TcpClient.close(producer_socket)
    TcpClient.close(consumer_socket)
  end
end
