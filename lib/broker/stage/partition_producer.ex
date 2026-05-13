defmodule Broker.Stage.PartitionProducer do
  @moduledoc false
  use GenStage

  alias Broker.Log.SegmentManager

  # Fallback poll interval (ms) when at the tail and no :new_records notification arrives.
  @poll_ms 100

  def start_link({topic, partition, start_offset}) do
    GenStage.start_link(__MODULE__, {topic, partition, start_offset})
  end

  # Called by Partition after each successful append so producers wake up immediately.
  def notify(topic, partition) do
    Registry.dispatch(Broker.ProducerRegistry, {topic, partition}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :new_records)
    end)
  end

  # ---------------------------------------------------------------------------
  # GenStage callbacks
  # ---------------------------------------------------------------------------

  @impl GenStage
  def init({topic, partition, start_offset}) do
    Registry.register(Broker.ProducerRegistry, {topic, partition}, nil)

    {:producer,
     %{
       topic: topic,
       partition: partition,
       offset: start_offset,
       demand: 0
     }}
  end

  @impl GenStage
  def handle_demand(demand, state) do
    dispatch(%{state | demand: state.demand + demand})
  end

  @impl GenStage
  def handle_info(:new_records, state), do: dispatch(state)
  def handle_info(:poll, state), do: dispatch(state)

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp dispatch(%{demand: 0} = state), do: {:noreply, [], state}

  defp dispatch(%{topic: topic, partition: partition, offset: offset, demand: demand} = state) do
    case Registry.lookup(Broker.Registry, {:mgr, topic, partition}) do
      [] ->
        # Partition not yet created — wait for it.
        Process.send_after(self(), :poll, @poll_ms)
        {:noreply, [], state}

      [{mgr_pid, _}] ->
        case SegmentManager.fetch(mgr_pid, offset, demand * 8192) do
          {:ok, []} ->
            Process.send_after(self(), :poll, @poll_ms)
            {:noreply, [], state}

          {:ok, records} ->
            to_emit = Enum.take(records, demand)
            new_offset = List.last(to_emit).offset + 1
            new_demand = demand - length(to_emit)
            new_state = %{state | offset: new_offset, demand: new_demand}
            if new_demand > 0, do: Process.send_after(self(), :poll, @poll_ms)
            {:noreply, to_emit, new_state}
        end
    end
  end
end
