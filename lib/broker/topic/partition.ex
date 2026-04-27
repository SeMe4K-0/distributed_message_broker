defmodule Broker.Topic.Partition do
  @moduledoc false
  use GenServer, restart: :permanent

  alias Broker.Log.SegmentManager

  @type server :: GenServer.server()
  @type record :: %{offset: integer(), timestamp: integer(), key: binary(), value: binary()}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link({topic, partition, opts}) do
    GenServer.start_link(__MODULE__, {topic, partition, opts}, name: via(topic, partition))
  end

  @spec append(server(), binary(), binary()) :: {:ok, integer()} | {:error, term()}
  def append(server, key, value), do: GenServer.call(server, {:append, key, value})

  @spec fetch(server(), integer(), integer()) :: {:ok, [record()]}
  def fetch(server, offset, max_bytes), do: GenServer.call(server, {:fetch, offset, max_bytes})

  @spec high_watermark(server()) :: integer()
  def high_watermark(server), do: GenServer.call(server, :high_watermark)

  def via(topic, partition) do
    {:via, Registry, {Broker.Registry, {topic, partition}}}
  end

  def manager_via(topic, partition) do
    {:via, Registry, {Broker.Registry, {:mgr, topic, partition}}}
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init({topic, partition, _opts}) do
    {:ok, %{topic: topic, partition: partition}}
  end

  @impl GenServer
  def handle_call({:append, key, value}, _from, state) do
    {:reply, SegmentManager.append(manager(state), key, value), state}
  end

  @impl GenServer
  def handle_call({:fetch, offset, max_bytes}, _from, state) do
    {:reply, SegmentManager.fetch(manager(state), offset, max_bytes), state}
  end

  @impl GenServer
  def handle_call(:high_watermark, _from, state) do
    {:reply, SegmentManager.high_watermark(manager(state)), state}
  end

  defp manager(%{topic: t, partition: p}), do: manager_via(t, p)
end
