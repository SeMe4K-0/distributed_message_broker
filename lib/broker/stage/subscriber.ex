defmodule Broker.Stage.Subscriber do
  @moduledoc false
  use GenStage

  alias Broker.Protocol.Codec

  def start_link({producer_pid, max_in_flight, sub_id, socket}) do
    GenStage.start_link(__MODULE__, {producer_pid, max_in_flight, sub_id, socket})
  end

  @impl GenStage
  def init({producer_pid, max_in_flight, sub_id, socket}) do
    {:consumer, %{sub_id: sub_id, socket: socket},
     subscribe_to: [{producer_pid, max_demand: max_in_flight, min_demand: 0}]}
  end

  @impl GenStage
  def handle_events(records, _from, state) do
    for record <- records do
      :gen_tcp.send(state.socket, Codec.encode_record_push(state.sub_id, record))
    end

    {:noreply, [], state}
  end
end
