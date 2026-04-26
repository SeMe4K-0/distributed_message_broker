defmodule Broker.Protocol.CodecTest do
  use ExUnit.Case, async: true

  alias Broker.Protocol.Codec

  describe "encode_frame / decode_frame round-trip" do
    test "PRODUCE_ACK" do
      payload = <<1::64, 42::64, 0>>
      frame = Codec.encode_frame(0x02, payload)
      assert {:ok, {0x02, ^payload}} = Codec.decode_frame(frame)
    end

    test "decode_frame returns :incomplete for partial data" do
      assert {:error, :incomplete} = Codec.decode_frame(<<0x42, 0x01, 0x01, 0x00, 100::32>>)
    end

    test "decode_frame returns :invalid_magic for bad magic" do
      assert {:error, :invalid_magic} = Codec.decode_frame(<<0xFF, 0x01, 0x01, 0x00, 0::32>>)
    end
  end

  describe "decode_produce" do
    test "single record no headers" do
      topic = "test-topic"
      key = "k1"
      value = "hello"

      payload =
        <<byte_size(topic)::16, topic::binary,
          0::32,
          999::64,
          1::32,
          byte_size(key)::32, key::binary,
          byte_size(value)::32, value::binary,
          0::32>>

      assert {:ok, decoded} = Codec.decode_produce(payload)
      assert decoded.topic == topic
      assert decoded.partition == 0
      assert decoded.correlation_id == 999
      assert [%{key: ^key, value: ^value}] = decoded.records
    end

    test "returns error for truncated payload" do
      assert {:error, _} = Codec.decode_produce(<<0, 3, "abc">>)
    end
  end

  describe "decode_fetch" do
    test "parses correctly" do
      topic = "events"
      payload =
        <<byte_size(topic)::16, topic::binary,
          2::32,
          100::64,
          65536::32,
          42::64>>

      assert {:ok, decoded} = Codec.decode_fetch(payload)
      assert decoded.topic == topic
      assert decoded.partition == 2
      assert decoded.offset == 100
      assert decoded.max_bytes == 65536
      assert decoded.correlation_id == 42
    end
  end

  describe "encode_produce_ack" do
    test "encodes into a valid frame" do
      frame = Codec.encode_produce_ack(7, 100, 0)
      assert {:ok, {0x02, payload}} = Codec.decode_frame(frame)
      assert <<7::64, 100::64, 0>> = payload
    end
  end
end
