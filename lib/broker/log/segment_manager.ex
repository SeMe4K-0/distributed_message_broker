defmodule Broker.Log.SegmentManager do
  @moduledoc false
  use GenServer, restart: :permanent

  require Logger

  alias Broker.Log.Segment

  @default_max_bytes 64 * 1024 * 1024

  @type record :: %{offset: integer(), timestamp: integer(), key: binary(), value: binary()}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link({topic, partition, opts}) do
    name = {:via, Registry, {Broker.Registry, {:mgr, topic, partition}}}
    GenServer.start_link(__MODULE__, {topic, partition, opts}, name: name)
  end

  @spec append(GenServer.server(), binary(), binary()) :: {:ok, integer()} | {:error, term()}
  def append(server, key, value), do: GenServer.call(server, {:append, key, value})

  @spec fetch(GenServer.server(), integer(), integer()) :: {:ok, [record()]}
  def fetch(server, offset, max_bytes), do: GenServer.call(server, {:fetch, offset, max_bytes})

  @spec high_watermark(GenServer.server()) :: integer()
  def high_watermark(server), do: GenServer.call(server, :high_watermark)

  # Force rotation of the active segment (used by Compactor and tests)
  @spec rotate(GenServer.server()) :: :ok
  def rotate(server), do: GenServer.call(server, :rotate)

  @spec segment_count(GenServer.server()) :: non_neg_integer()
  def segment_count(server), do: GenServer.call(server, :segment_count)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init({topic, partition, opts}) do
    data_dir = Keyword.get(opts, :data_dir, Application.get_env(:broker, :data_dir, "data"))
    max_bytes = Keyword.get(opts, :max_segment_bytes, @default_max_bytes)
    dir = Path.join([data_dir, topic, "#{partition}"])
    File.mkdir_p!(dir)

    segments = recover_segments(dir, max_bytes)
    active = ensure_active_segment(segments, dir, max_bytes)

    Logger.debug("[SegmentManager #{topic}/#{partition}] #{length(segments)} segments, active base=#{Segment.base_offset(active)}")

    {:ok,
     %{
       dir: dir,
       max_bytes: max_bytes,
       # completed (sealed) segments — list of {base_offset, pid}, sorted ascending
       sealed: build_sealed_list(segments, active),
       active: active
     }}
  end

  @impl GenServer
  def handle_call({:append, key, value}, _from, state) do
    state = maybe_rotate(state)

    case Segment.append(state.active, key, value) do
      {:ok, _offset} = ok ->
        {:reply, ok, state}

      {:error, :segment_full} ->
        # Rotate and retry — should not happen after maybe_rotate, but guard anyway
        new_state = do_rotate(state)

        case Segment.append(new_state.active, key, value) do
          {:ok, _} = ok -> {:reply, ok, new_state}
          err -> {:reply, err, new_state}
        end

      err ->
        {:reply, err, state}
    end
  end

  @impl GenServer
  def handle_call({:fetch, offset, max_bytes}, _from, state) do
    records = collect_records(state, offset, max_bytes)
    {:reply, {:ok, records}, state}
  end

  @impl GenServer
  def handle_call(:high_watermark, _from, state) do
    {:reply, Segment.next_offset(state.active), state}
  end

  @impl GenServer
  def handle_call(:rotate, _from, state) do
    new_state = do_rotate(state)
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call(:segment_count, _from, state) do
    {:reply, length(state.sealed) + 1, state}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp maybe_rotate(%{active: active} = state) do
    if Segment.full?(active), do: do_rotate(state), else: state
  end

  defp do_rotate(%{active: active, sealed: sealed, dir: dir, max_bytes: max_bytes} = state) do
    base = Segment.base_offset(active)
    Logger.debug("[SegmentManager] Rotating segment base=#{base}")

    new_base = Segment.next_offset(active)
    {:ok, new_segment} = Segment.start_link({new_base, dir, [max_bytes: max_bytes]})

    %{state | sealed: sealed ++ [{base, active}], active: new_segment}
  end

  # Walk segments from oldest to newest, collecting records starting at `offset`.
  defp collect_records(%{sealed: sealed, active: active}, start_offset, max_bytes) do
    all_segments = Enum.map(sealed, &elem(&1, 1)) ++ [active]

    # Skip segments that end before start_offset
    relevant =
      Enum.filter(all_segments, fn seg ->
        Segment.next_offset(seg) > start_offset
      end)

    Enum.reduce_while(relevant, {[], 0}, fn seg, {acc, bytes} ->
      base = Segment.base_offset(seg)
      next = Segment.next_offset(seg)

      # Read all entries from this segment that are >= start_offset
      from = max(start_offset, base)

      {new_acc, new_bytes, done} =
        read_range(seg, from, next - 1, acc, bytes, max_bytes)

      if done, do: {:halt, {new_acc, new_bytes}}, else: {:cont, {new_acc, new_bytes}}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  defp read_range(_seg, from, to, acc, bytes, _max) when from > to, do: {acc, bytes, false}

  defp read_range(seg, from, to, acc, bytes, max_bytes) do
    case Segment.read_at(seg, from) do
      {:ok, entry} ->
        size = byte_size(entry.key) + byte_size(entry.value) + 32

        if bytes + size > max_bytes and acc != [] do
          {acc, bytes, true}
        else
          read_range(seg, from + 1, to, [entry | acc], bytes + size, max_bytes)
        end

      {:error, :not_found} ->
        # Gap or end of segment — try next offset
        read_range(seg, from + 1, to, acc, bytes, max_bytes)
    end
  end

  # On startup, find existing .log files and open them as sealed segments.
  defp recover_segments(dir, max_bytes) do
    dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".log"))
    |> Enum.sort()
    |> Enum.map(fn filename ->
      base = filename |> String.replace_suffix(".log", "") |> String.to_integer()
      {:ok, pid} = Segment.start_link({base, dir, [max_bytes: max_bytes]})
      pid
    end)
  end

  # If there are no segments, or the last segment is full, open a new one.
  defp ensure_active_segment([], dir, max_bytes) do
    {:ok, pid} = Segment.start_link({0, dir, [max_bytes: max_bytes]})
    pid
  end

  defp ensure_active_segment(segments, dir, max_bytes) do
    last = List.last(segments)

    if Segment.full?(last) do
      new_base = Segment.next_offset(last)
      {:ok, pid} = Segment.start_link({new_base, dir, [max_bytes: max_bytes]})
      pid
    else
      last
    end
  end

  # Build the sealed list from all recovered segments except the active one.
  defp build_sealed_list([], _active), do: []

  defp build_sealed_list(segments, active) do
    segments
    |> Enum.reject(&(&1 == active))
    |> Enum.map(fn seg -> {Segment.base_offset(seg), seg} end)
  end
end
