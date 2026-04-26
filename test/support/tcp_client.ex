defmodule Test.TcpClient do
  @moduledoc false

  alias Broker.Protocol.Codec

  def connect(port \\ 9092) do
    :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: :raw, active: false], 5_000)
  end

  def close(socket), do: :gen_tcp.close(socket)

  def produce(socket, topic, partition, records, correlation_id \\ 1) do
    records_bin =
      Enum.map_join(records, fn {key, value} ->
        <<byte_size(key)::32, key::binary, byte_size(value)::32, value::binary, 0::32>>
      end)

    payload =
      <<byte_size(topic)::16, topic::binary,
        partition::32,
        correlation_id::64,
        length(records)::32>> <> records_bin

    frame = Codec.encode_frame(0x01, payload)
    :gen_tcp.send(socket, frame)
    recv_frame(socket)
  end

  def fetch(socket, topic, partition, offset, max_bytes \\ 1_048_576, correlation_id \\ 1) do
    payload =
      <<byte_size(topic)::16, topic::binary,
        partition::32,
        offset::64,
        max_bytes::32,
        correlation_id::64>>

    frame = Codec.encode_frame(0x03, payload)
    :gen_tcp.send(socket, frame)
    recv_frame(socket)
  end

  def recv_frame(socket, timeout \\ 5_000) do
    with {:ok, data} <- :gen_tcp.recv(socket, 0, timeout) do
      Codec.decode_frame(data)
    end
  end
end
