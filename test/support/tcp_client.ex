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

  def subscribe(socket, topic, partition, start_offset, max_in_flight, sub_id) do
    payload =
      <<byte_size(topic)::16, topic::binary,
        partition::32,
        start_offset::64,
        max_in_flight::32,
        sub_id::64>>

    frame = Codec.encode_frame(0x0B, payload)
    :gen_tcp.send(socket, frame)
    recv_frame(socket)
  end

  def unsubscribe(socket, sub_id) do
    frame = Codec.encode_frame(0x0D, <<sub_id::64>>)
    :gen_tcp.send(socket, frame)
  end

  # Reads exactly one frame, looping to accumulate bytes if the first read is incomplete.
  # Note: any bytes past the first complete frame are discarded — use recv_n_frames for streaming.
  def recv_frame(socket, timeout \\ 5_000) do
    recv_frame_buf(socket, <<>>, timeout)
  end

  defp recv_frame_buf(socket, buf, timeout) do
    case Codec.frame_size(buf) do
      {:ok, size} ->
        <<frame::binary-size(size), _rest::binary>> = buf
        Codec.decode_frame(frame)

      {:error, :incomplete} ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, data} -> recv_frame_buf(socket, buf <> data, timeout)
          err -> err
        end
    end
  end

  # Reads exactly n frames from the socket, properly handling the case where
  # multiple frames arrive in a single TCP segment.
  def recv_n_frames(socket, n, timeout \\ 5_000) do
    recv_n_frames_buf(socket, <<>>, n, [], timeout)
  end

  defp recv_n_frames_buf(_socket, _buf, 0, acc, _timeout) do
    {:ok, Enum.reverse(acc)}
  end

  defp recv_n_frames_buf(socket, buf, remaining, acc, timeout) do
    case Codec.frame_size(buf) do
      {:ok, size} ->
        <<frame::binary-size(size), rest::binary>> = buf
        {:ok, decoded} = Codec.decode_frame(frame)
        recv_n_frames_buf(socket, rest, remaining - 1, [decoded | acc], timeout)

      {:error, :incomplete} ->
        case :gen_tcp.recv(socket, 0, timeout) do
          {:ok, data} -> recv_n_frames_buf(socket, buf <> data, remaining, acc, timeout)
          err -> err
        end
    end
  end

  # Decode a RECORD_PUSH (0x0E) payload
  def decode_record_push(<<sub_id::64, offset::64, timestamp::64,
                           key_len::32, key::binary-size(key_len),
                           val_len::32, value::binary-size(val_len)>>) do
    %{sub_id: sub_id, offset: offset, timestamp: timestamp, key: key, value: value}
  end
end
