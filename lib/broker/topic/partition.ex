defmodule Broker.Topic.Partition do
  @moduledoc false
  use GenServer, restart: :permanent

  require Logger

  alias Broker.Log.WAL

  @type server :: GenServer.server()
  @type record :: %{offset: integer(), timestamp: integer(), key: binary(), value: binary()}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link({topic, partition, opts}) do
    name = via(topic, partition)
    GenServer.start_link(__MODULE__, {topic, partition, opts}, name: name)
  end

  @spec append(server(), binary(), binary()) :: {:ok, integer()} | {:error, term()}
  def append(server, key, value) do
    GenServer.call(server, {:append, key, value})
  end

  @spec fetch(server(), integer(), integer()) :: {:ok, [record()]}
  def fetch(server, offset, max_bytes) do
    GenServer.call(server, {:fetch, offset, max_bytes})
  end

  @spec high_watermark(server()) :: integer()
  def high_watermark(server) do
    GenServer.call(server, :high_watermark)
  end

  def via(topic, partition) do
    {:via, Registry, {Broker.Registry, {topic, partition}}}
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init({topic, partition, opts}) do
    data_dir = Keyword.get(opts, :data_dir, Application.get_env(:broker, :data_dir, "data"))
    dir = Path.join([data_dir, topic, "#{partition}"])
    File.mkdir_p!(dir)
    log_path = Path.join(dir, "00000000000000000000.log")

    {entries, next_offset} = recover_from_disk(log_path)
    Logger.debug("[Partition #{topic}/#{partition}] Recovered #{length(entries)} entries, next_offset=#{next_offset}")

    {:ok,
     %{
       topic: topic,
       partition: partition,
       log_path: log_path,
       entries: entries,
       next_offset: next_offset
     }}
  end

  @impl GenServer
  def handle_call({:append, key, value}, _from, state) do
    offset = state.next_offset
    bin = WAL.encode_entry(offset, key, value)

    case WAL.append_to_file(state.log_path, bin) do
      :ok ->
        entry = %{offset: offset, timestamp: System.system_time(:millisecond), key: key, value: value}
        new_state = %{state | entries: state.entries ++ [entry], next_offset: offset + 1}
        {:reply, {:ok, offset}, new_state}

      err ->
        {:reply, err, state}
    end
  end

  @impl GenServer
  def handle_call({:fetch, offset, max_bytes}, _from, state) do
    records = fetch_from_entries(state.entries, offset, max_bytes)
    {:reply, {:ok, records}, state}
  end

  @impl GenServer
  def handle_call(:high_watermark, _from, state) do
    {:reply, state.next_offset, state}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp recover_from_disk(log_path) do
    case WAL.read_all(log_path) do
      {:ok, []} ->
        {[], 0}

      {:ok, entries} ->
        next = (List.last(entries).offset) + 1
        {entries, next}

      {:error, reason} ->
        Logger.error("[Partition] WAL recovery failed: #{inspect(reason)}")
        {[], 0}
    end
  end

  defp fetch_from_entries(entries, offset, max_bytes) do
    entries
    |> Enum.drop_while(&(&1.offset < offset))
    |> Enum.reduce_while({[], 0}, fn entry, {acc, bytes} ->
      size = byte_size(entry.key) + byte_size(entry.value) + 32
      if bytes + size > max_bytes and acc != [] do
        {:halt, {acc, bytes}}
      else
        {:cont, {[entry | acc], bytes + size}}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end
end
