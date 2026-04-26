defmodule Broker.Integration.ProduceConsumeTest do
  use ExUnit.Case

  alias Test.TcpClient

  @port 19092

  setup_all do
    dir = System.tmp_dir!() |> Path.join("integration_#{:rand.uniform(1_000_000)}")
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

  test "PRODUCE then FETCH returns the same records" do
    {:ok, socket} = TcpClient.connect(@port)

    {:ok, ack_frame} = TcpClient.produce(socket, "events", 0, [{"key1", "hello"}, {"key2", "world"}])
    assert {0x02, <<_cid::64, base_offset::64, 0>>} = ack_frame
    assert base_offset == 0

    {:ok, fetch_frame} = TcpClient.fetch(socket, "events", 0, 0)
    assert {0x04, payload} = fetch_frame

    <<_cid::64, _hwm::64, num::32, records_bin::binary>> = payload
    assert num == 2

    <<0::64, _ts::64, 4::32, "key1"::binary, 5::32, "hello"::binary, rest::binary>> = records_bin
    <<1::64, _ts2::64, 4::32, "key2"::binary, 5::32, "world"::binary>> = rest

    TcpClient.close(socket)
  end

  test "FETCH from mid-offset skips earlier records" do
    {:ok, socket} = TcpClient.connect(@port)

    TcpClient.produce(socket, "seq", 0, [{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}])

    {:ok, fetch_frame} = TcpClient.fetch(socket, "seq", 0, 1)
    {0x04, <<_cid::64, _hwm::64, num::32, _::binary>>} = fetch_frame
    assert num == 2

    TcpClient.close(socket)
  end

  test "FETCH for unknown topic returns error frame" do
    {:ok, socket} = TcpClient.connect(@port)

    {:ok, frame} = TcpClient.fetch(socket, "no-such-topic", 0, 0)
    assert {0xFF, _payload} = frame

    TcpClient.close(socket)
  end
end
