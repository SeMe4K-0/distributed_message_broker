defmodule Broker.Log.Segment do
  @moduledoc false
  use GenServer, restart: :permanent

  require Logger

  alias Broker.Log.{WAL, SegmentIndex}

  # Default max segment size: 64 MB
  @default_max_bytes 64 * 1024 * 1024

  @type record :: %{offset: integer(), timestamp: integer(), key: binary(), value: binary()}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link({base_offset, dir, opts}) do
    GenServer.start_link(__MODULE__, {base_offset, dir, opts})
  end

  @spec append(GenServer.server(), binary(), binary()) ::
          {:ok, offset :: integer()} | {:error, :segment_full} | {:error, term()}
  def append(server, key, value), do: GenServer.call(server, {:append, key, value})

  @spec read_at(GenServer.server(), offset :: integer()) ::
          {:ok, record()} | {:error, :not_found}
  def read_at(server, offset), do: GenServer.call(server, {:read_at, offset})

  @spec full?(GenServer.server()) :: boolean()
  def full?(server), do: GenServer.call(server, :full?)

  @spec base_offset(GenServer.server()) :: integer()
  def base_offset(server), do: GenServer.call(server, :base_offset)

  @spec next_offset(GenServer.server()) :: integer()
  def next_offset(server), do: GenServer.call(server, :next_offset)

  @spec close(GenServer.server()) :: :ok
  def close(server), do: GenServer.call(server, :close)

  @spec log_path(GenServer.server()) :: Path.t()
  def log_path(server), do: GenServer.call(server, :log_path)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init({base_offset, dir, opts}) do
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)
    log_path = segment_path(dir, base_offset, ".log")
    idx_path = segment_path(dir, base_offset, ".index")

    # Recover existing state from disk
    {entries, write_pos} = recover_log(log_path)
    {:ok, idx_data} = SegmentIndex.read_all(idx_path)

    next = if entries == [], do: base_offset, else: List.last(entries).offset + 1

    Logger.debug("[Segment base=#{base_offset}] Recovered #{length(entries)} entries, write_pos=#{write_pos}")

    {:ok,
     %{
       base_offset: base_offset,
       next_offset: next,
       log_path: log_path,
       idx_path: idx_path,
       write_pos: write_pos,
       index_data: idx_data,
       max_bytes: max_bytes
     }}
  end

  @impl GenServer
  def handle_call({:append, key, value}, _from, state) do
    if state.write_pos >= state.max_bytes do
      {:reply, {:error, :segment_full}, state}
    else
      offset = state.next_offset
      entry_bin = WAL.encode_entry(offset, key, value)
      relative_offset = offset - state.base_offset

      with :ok <- WAL.append_to_file(state.log_path, entry_bin),
           :ok <- SegmentIndex.append_to_file(state.idx_path, relative_offset, state.write_pos) do
        new_idx_entry = SegmentIndex.build_entry(relative_offset, state.write_pos)

        new_state = %{
          state
          | next_offset: offset + 1,
            write_pos: state.write_pos + byte_size(entry_bin),
            index_data: state.index_data <> new_idx_entry
        }

        {:reply, {:ok, offset}, new_state}
      else
        err -> {:reply, err, state}
      end
    end
  end

  @impl GenServer
  def handle_call({:read_at, offset}, _from, state) do
    relative = offset - state.base_offset

    case SegmentIndex.binary_search(state.index_data, relative) do
      :not_found ->
        {:reply, {:error, :not_found}, state}

      {:ok, file_pos} ->
        case read_entry_at(state.log_path, file_pos) do
          {:ok, entry} -> {:reply, {:ok, entry}, state}
          err -> {:reply, err, state}
        end
    end
  end

  @impl GenServer
  def handle_call(:full?, _from, state) do
    {:reply, state.write_pos >= state.max_bytes, state}
  end

  @impl GenServer
  def handle_call(:base_offset, _from, state), do: {:reply, state.base_offset, state}

  @impl GenServer
  def handle_call(:next_offset, _from, state), do: {:reply, state.next_offset, state}

  @impl GenServer
  def handle_call(:log_path, _from, state), do: {:reply, state.log_path, state}

  @impl GenServer
  def handle_call(:close, _from, state), do: {:stop, :normal, :ok, state}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp segment_path(dir, base_offset, ext) do
    name = base_offset |> Integer.to_string() |> String.pad_leading(20, "0")
    Path.join(dir, name <> ext)
  end

  defp recover_log(log_path) do
    case WAL.scan_file(log_path) do
      {:ok, entries, consumed} -> {entries, consumed}
      _ -> {[], 0}
    end
  end

  defp read_entry_at(log_path, file_pos) do
    # Read the 4-byte length prefix first, then the full entry payload
    case :file.open(log_path, [:read, :binary, :raw]) do
      {:ok, fd} ->
        result =
          with {:ok, <<entry_len::32>>} <- :file.pread(fd, file_pos, 4),
               {:ok, payload} <- :file.pread(fd, file_pos + 4, entry_len) do
            decode_entry_payload(payload)
          end

        :file.close(fd)
        result

      err ->
        err
    end
  end

  defp decode_entry_payload(<<offset::64, ts::64, stored_crc::32, key_len::32,
                               key::binary-size(key_len), val_len::32,
                               value::binary-size(val_len)>>) do
    crc_data =
      <<offset::64, ts::64, key_len::32, key::binary, val_len::32, value::binary>>

    if :erlang.crc32(crc_data) == stored_crc do
      {:ok, %{offset: offset, timestamp: ts, key: key, value: value}}
    else
      {:error, :checksum_mismatch}
    end
  end

  defp decode_entry_payload(_), do: {:error, :corrupt_entry}
end
