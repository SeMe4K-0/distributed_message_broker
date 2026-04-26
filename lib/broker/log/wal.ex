defmodule Broker.Log.WAL do
  @moduledoc false

  # On-disk entry layout (all integers big-endian):
  #   entry_len   :: 32   (total bytes of the payload that follows)
  #   offset      :: 64
  #   timestamp   :: 64   (unix ms)
  #   crc32       :: 32   (covers offset + timestamp + key + value bytes)
  #   key_len     :: 32
  #   key         :: binary
  #   value_len   :: 32
  #   value       :: binary

  @spec encode_entry(offset :: integer(), key :: binary(), value :: binary()) :: binary()
  def encode_entry(offset, key, value) when is_binary(key) and is_binary(value) do
    ts = System.system_time(:millisecond)

    crc_data =
      <<offset::64, ts::64, byte_size(key)::32, key::binary,
        byte_size(value)::32, value::binary>>

    crc = :erlang.crc32(crc_data)

    payload =
      <<offset::64, ts::64, crc::32, byte_size(key)::32, key::binary,
        byte_size(value)::32, value::binary>>

    <<byte_size(payload)::32, payload::binary>>
  end

  @spec decode_entry(binary()) ::
          {:ok, map(), rest :: binary()}
          | {:error, :incomplete}
          | {:error, :checksum_mismatch}
  def decode_entry(<<entry_len::32, payload::binary-size(entry_len), rest::binary>>) do
    case payload do
      <<offset::64, ts::64, stored_crc::32, key_len::32, key::binary-size(key_len),
        val_len::32, value::binary-size(val_len)>> ->
        crc_data =
          <<offset::64, ts::64, key_len::32, key::binary, val_len::32, value::binary>>

        if :erlang.crc32(crc_data) == stored_crc do
          {:ok, %{offset: offset, timestamp: ts, key: key, value: value}, rest}
        else
          {:error, :checksum_mismatch}
        end

      _ ->
        {:error, :incomplete}
    end
  end

  def decode_entry(<<_partial::binary>>) do
    {:error, :incomplete}
  end

  @spec decode_all(binary()) :: {:ok, [map()]} | {:error, atom()}
  def decode_all(data), do: decode_all(data, [])

  defp decode_all(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_all(data, acc) do
    case decode_entry(data) do
      {:ok, entry, rest} -> decode_all(rest, [entry | acc])
      {:error, :incomplete} -> {:ok, Enum.reverse(acc)}
      err -> err
    end
  end

  @spec append_to_file(Path.t(), binary()) :: :ok | {:error, term()}
  def append_to_file(path, data) when is_binary(data) do
    case :file.open(path, [:append, :binary, :raw]) do
      {:ok, fd} ->
        result = :file.write(fd, data)
        :file.close(fd)
        result

      err ->
        err
    end
  end

  @spec read_all(Path.t()) :: {:ok, [map()]} | {:error, term()}
  def read_all(path) do
    case File.read(path) do
      {:ok, data} -> decode_all(data)
      {:error, :enoent} -> {:ok, []}
      err -> err
    end
  end

  # Returns {entries, byte_size_of_valid_data}
  @spec scan_file(Path.t()) :: {:ok, [map()], non_neg_integer()} | {:error, term()}
  def scan_file(path) do
    case File.read(path) do
      {:ok, data} ->
        {entries, consumed} = scan_binary(data, [], 0)
        {:ok, entries, consumed}

      {:error, :enoent} ->
        {:ok, [], 0}

      err ->
        err
    end
  end

  defp scan_binary(<<entry_len::32, payload::binary-size(entry_len), rest::binary>>, acc, pos) do
    frame_size = 4 + entry_len

    case payload do
      <<offset::64, ts::64, stored_crc::32, key_len::32, key::binary-size(key_len),
        val_len::32, value::binary-size(val_len)>> ->
        crc_data =
          <<offset::64, ts::64, key_len::32, key::binary, val_len::32, value::binary>>

        if :erlang.crc32(crc_data) == stored_crc do
          entry = %{offset: offset, timestamp: ts, key: key, value: value}
          scan_binary(rest, [entry | acc], pos + frame_size)
        else
          {Enum.reverse(acc), pos}
        end

      _ ->
        {Enum.reverse(acc), pos}
    end
  end

  defp scan_binary(_, acc, pos), do: {Enum.reverse(acc), pos}
end
