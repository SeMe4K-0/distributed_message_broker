defmodule Broker.Log.SegmentIndex do
  @moduledoc false

  # Index file format: fixed 16-byte entries, big-endian.
  #   relative_offset :: 64   (offset - base_offset of the segment)
  #   file_position   :: 64   (byte position of the entry in the .log file)
  #
  # Files are kept sorted by relative_offset, so binary search gives O(log n).

  @entry_size 16

  @spec build_entry(relative_offset :: non_neg_integer(), file_position :: non_neg_integer()) ::
          binary()
  def build_entry(relative_offset, file_position) do
    <<relative_offset::64, file_position::64>>
  end

  @spec decode_entry(binary()) :: {relative_offset :: non_neg_integer(), file_position :: non_neg_integer()}
  def decode_entry(<<relative_offset::64, file_position::64>>) do
    {relative_offset, file_position}
  end

  @spec entry_count(index_binary :: binary()) :: non_neg_integer()
  def entry_count(data), do: div(byte_size(data), @entry_size)

  # Returns the file_position for the largest stored relative_offset <= target.
  # :not_found when the index is empty or target < first entry.
  @spec binary_search(index_binary :: binary(), relative_offset :: non_neg_integer()) ::
          {:ok, file_position :: non_neg_integer()} | :not_found
  def binary_search(<<>>, _target), do: :not_found

  def binary_search(data, target) do
    count = entry_count(data)
    do_search(data, target, 0, count - 1, :not_found)
  end

  defp do_search(_data, _target, low, high, best) when low > high, do: best

  defp do_search(data, target, low, high, best) do
    mid = div(low + high, 2)
    {rel_off, file_pos} = entry_at(data, mid)

    cond do
      rel_off == target ->
        {:ok, file_pos}

      rel_off < target ->
        do_search(data, target, mid + 1, high, {:ok, file_pos})

      rel_off > target ->
        do_search(data, target, low, mid - 1, best)
    end
  end

  defp entry_at(data, idx) do
    pos = idx * @entry_size
    <<_::binary-size(pos), entry::binary-size(@entry_size), _::binary>> = data
    decode_entry(entry)
  end

  @spec append_to_file(Path.t(), non_neg_integer(), non_neg_integer()) :: :ok | {:error, term()}
  def append_to_file(path, relative_offset, file_position) do
    entry = build_entry(relative_offset, file_position)

    case :file.open(path, [:append, :binary, :raw]) do
      {:ok, fd} ->
        result = :file.write(fd, entry)
        :file.close(fd)
        result

      err ->
        err
    end
  end

  @spec read_all(Path.t()) :: {:ok, binary()} | {:error, term()}
  def read_all(path) do
    case File.read(path) do
      {:ok, data} -> {:ok, data}
      {:error, :enoent} -> {:ok, <<>>}
      err -> err
    end
  end
end
