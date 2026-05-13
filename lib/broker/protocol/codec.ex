defmodule Broker.Protocol.Codec do
  @moduledoc false

  alias Broker.Protocol.Frame

  # ---------------------------------------------------------------------------
  # Frame envelope
  # ---------------------------------------------------------------------------

  @spec decode_frame(binary()) ::
          {:ok, {type :: byte(), payload :: binary()}}
          | {:error, :incomplete}
          | {:error, :invalid_magic}
  def decode_frame(<<0x42, 0x01, type, _flags, length::32, payload::binary-size(length), _rest::binary>>) do
    {:ok, {type, payload}}
  end

  def decode_frame(<<0x42, 0x01, _::binary>>) do
    {:error, :incomplete}
  end

  def decode_frame(<<>>) do
    {:error, :incomplete}
  end

  def decode_frame(_) do
    {:error, :invalid_magic}
  end

  # How many bytes a complete frame occupies in the buffer (for advancing the cursor)
  @spec frame_size(binary()) :: {:ok, non_neg_integer()} | {:error, :incomplete}
  def frame_size(<<0x42, 0x01, _type, _flags, length::32, _::binary-size(length), _rest::binary>>) do
    {:ok, 8 + length}
  end

  def frame_size(_), do: {:error, :incomplete}

  @spec encode_frame(type :: byte(), payload :: binary()) :: binary()
  def encode_frame(type, payload) when is_binary(payload) do
    <<Frame.magic(), Frame.version(), type, 0x00, byte_size(payload)::32, payload::binary>>
  end

  # ---------------------------------------------------------------------------
  # PRODUCE (0x01)
  # ---------------------------------------------------------------------------

  @spec decode_produce(binary()) :: {:ok, map()} | {:error, atom()}
  def decode_produce(<<
        topic_len::16,
        topic::binary-size(topic_len),
        partition::32,
        correlation_id::64,
        num_records::32,
        rest::binary
      >>) do
    case decode_records(rest, num_records, []) do
      {:ok, records} ->
        {:ok,
         %{
           topic: topic,
           partition: partition,
           correlation_id: correlation_id,
           records: records
         }}

      err ->
        err
    end
  end

  def decode_produce(_), do: {:error, :invalid_produce}

  defp decode_records(_, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_records(
         <<key_len::32, key::binary-size(key_len), val_len::32, value::binary-size(val_len),
           headers_count::32, rest::binary>>,
         n,
         acc
       ) do
    case decode_headers(rest, headers_count, []) do
      {:ok, headers, remaining} ->
        decode_records(remaining, n - 1, [%{key: key, value: value, headers: headers} | acc])

      err ->
        err
    end
  end

  defp decode_records(_, _, _), do: {:error, :truncated_records}

  defp decode_headers(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_headers(
         <<name_len::16, name::binary-size(name_len), val_len::32, val::binary-size(val_len),
           rest::binary>>,
         n,
         acc
       ) do
    decode_headers(rest, n - 1, [{name, val} | acc])
  end

  defp decode_headers(_, _, _), do: {:error, :truncated_headers}

  @spec encode_produce_ack(integer(), integer(), byte()) :: binary()
  def encode_produce_ack(correlation_id, base_offset, error_code) do
    encode_frame(Frame.produce_ack(), <<correlation_id::64, base_offset::64, error_code>>)
  end

  # ---------------------------------------------------------------------------
  # FETCH (0x03)
  # ---------------------------------------------------------------------------

  @spec decode_fetch(binary()) :: {:ok, map()} | {:error, atom()}
  def decode_fetch(<<
        topic_len::16,
        topic::binary-size(topic_len),
        partition::32,
        fetch_offset::64,
        max_bytes::32,
        correlation_id::64
      >>) do
    {:ok,
     %{
       topic: topic,
       partition: partition,
       offset: fetch_offset,
       max_bytes: max_bytes,
       correlation_id: correlation_id
     }}
  end

  def decode_fetch(_), do: {:error, :invalid_fetch}

  @spec encode_fetch_response(integer(), integer(), list()) :: binary()
  def encode_fetch_response(correlation_id, high_watermark, records) do
    records_bin = Enum.map_join(records, fn r ->
      <<r.offset::64, r.timestamp::64,
        byte_size(r.key)::32, r.key::binary,
        byte_size(r.value)::32, r.value::binary>>
    end)

    payload =
      <<correlation_id::64, high_watermark::64, length(records)::32>> <> records_bin

    encode_frame(Frame.fetch_response(), payload)
  end

  # ---------------------------------------------------------------------------
  # SUBSCRIBE (0x0B) / SUBSCRIBE_ACK (0x0C)
  # ---------------------------------------------------------------------------

  @spec decode_subscribe(binary()) :: {:ok, map()} | {:error, atom()}
  def decode_subscribe(<<
        topic_len::16,
        topic::binary-size(topic_len),
        partition::32,
        start_offset::64,
        max_in_flight::32,
        sub_id::64
      >>) do
    {:ok,
     %{
       topic: topic,
       partition: partition,
       start_offset: start_offset,
       max_in_flight: max_in_flight,
       sub_id: sub_id
     }}
  end

  def decode_subscribe(_), do: {:error, :invalid_subscribe}

  @spec encode_subscribe_ack(integer(), byte()) :: binary()
  def encode_subscribe_ack(sub_id, error_code) do
    encode_frame(Frame.subscribe_ack(), <<sub_id::64, error_code>>)
  end

  # ---------------------------------------------------------------------------
  # UNSUBSCRIBE (0x0D)
  # ---------------------------------------------------------------------------

  @spec decode_unsubscribe(binary()) :: {:ok, map()} | {:error, atom()}
  def decode_unsubscribe(<<sub_id::64>>) do
    {:ok, %{sub_id: sub_id}}
  end

  def decode_unsubscribe(_), do: {:error, :invalid_unsubscribe}

  # ---------------------------------------------------------------------------
  # RECORD_PUSH (0x0E)  — broker → client streaming record
  # ---------------------------------------------------------------------------

  @spec encode_record_push(integer(), map()) :: binary()
  def encode_record_push(sub_id, record) do
    payload =
      <<sub_id::64, record.offset::64, record.timestamp::64,
        byte_size(record.key)::32, record.key::binary,
        byte_size(record.value)::32, record.value::binary>>

    encode_frame(Frame.record_push(), payload)
  end

  # ---------------------------------------------------------------------------
  # ERROR (0xFF)
  # ---------------------------------------------------------------------------

  @spec encode_error(integer(), byte(), String.t()) :: binary()
  def encode_error(correlation_id, error_code, message) do
    msg_bin = message |> to_string() |> :unicode.characters_to_binary()
    payload = <<correlation_id::64, error_code, byte_size(msg_bin)::16, msg_bin::binary>>
    encode_frame(Frame.error(), payload)
  end
end
