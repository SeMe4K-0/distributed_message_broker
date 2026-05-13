defmodule Broker.Network.Connection do
  @moduledoc false
  use GenServer, restart: :temporary

  require Logger

  alias Broker.Protocol.{Codec, Frame}
  alias Broker.Stage.SubscriptionSupervisor
  alias Broker.Topic.{Partition, TopicSupervisor}

  @recv_timeout 30_000

  def start_link({socket, _opts}) do
    GenServer.start_link(__MODULE__, socket)
  end

  @impl GenServer
  def init(socket) do
    send(self(), :recv)
    {:ok, %{socket: socket, buffer: <<>>, subs: %{}}}
  end

  @impl GenServer
  def handle_info(:recv, %{socket: socket} = state) do
    case :gen_tcp.recv(socket, 0, @recv_timeout) do
      {:ok, data} ->
        new_buffer = state.buffer <> data
        {remaining, responses, new_state} = process_buffer(new_buffer, [], state)
        responses
        |> Enum.reject(&is_nil/1)
        |> Enum.each(&:gen_tcp.send(socket, &1))
        send(self(), :recv)
        {:noreply, %{new_state | buffer: remaining}}

      {:error, reason} when reason in [:closed, :econnreset, :timeout] ->
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("[Connection] Recv error: #{inspect(reason)}")
        {:stop, reason, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    for {_sub_id, {producer_pid, subscriber_pid}} <- state.subs do
      SubscriptionSupervisor.stop_subscription(producer_pid, subscriber_pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Buffer processing — extract complete frames one at a time
  # ---------------------------------------------------------------------------

  defp process_buffer(buffer, responses, state) do
    case Codec.frame_size(buffer) do
      {:ok, size} ->
        <<frame_data::binary-size(size), rest::binary>> = buffer
        <<_magic, _ver, type, _flags, _len::32, payload::binary>> = frame_data
        {response, new_state} = handle_frame(type, payload, state)
        process_buffer(rest, [response | responses], new_state)

      {:error, :incomplete} ->
        {buffer, Enum.reverse(responses), state}
    end
  end

  # ---------------------------------------------------------------------------
  # Frame handlers — return {response_binary | nil, state}
  # ---------------------------------------------------------------------------

  # PRODUCE (0x01)
  defp handle_frame(0x01, payload, state) do
    response =
      case Codec.decode_produce(payload) do
        {:ok, %{topic: topic, partition: p, correlation_id: cid, records: records}} ->
          partition_pid = ensure_partition(topic, p)

          case append_records(partition_pid, records) do
            {:ok, base_offset} -> Codec.encode_produce_ack(cid, base_offset, Frame.err_none())
            {:error, _} -> Codec.encode_error(cid, Frame.err_invalid_request(), "append failed")
          end

        {:error, _} ->
          Codec.encode_error(0, Frame.err_invalid_request(), "bad produce payload")
      end

    {response, state}
  end

  # FETCH (0x03)
  defp handle_frame(0x03, payload, state) do
    response =
      case Codec.decode_fetch(payload) do
        {:ok, %{topic: topic, partition: p, offset: offset, max_bytes: max_bytes, correlation_id: cid}} ->
          case get_partition(topic, p) do
            nil ->
              Codec.encode_error(cid, Frame.err_unknown_topic(), "unknown topic/partition")

            pid ->
              {:ok, records} = Partition.fetch(pid, offset, max_bytes)
              hwm = Partition.high_watermark(pid)
              Codec.encode_fetch_response(cid, hwm, records)
          end

        {:error, _} ->
          Codec.encode_error(0, Frame.err_invalid_request(), "bad fetch payload")
      end

    {response, state}
  end

  # SUBSCRIBE (0x0B)
  defp handle_frame(0x0B, payload, state) do
    case Codec.decode_subscribe(payload) do
      {:ok,
       %{
         topic: topic,
         partition: p,
         start_offset: start_offset,
         max_in_flight: max_in_flight,
         sub_id: sub_id
       }} ->
        _ = ensure_partition(topic, p)

        case SubscriptionSupervisor.start_subscription(
               topic, p, start_offset, max_in_flight, sub_id, state.socket
             ) do
          {:ok, producer_pid, subscriber_pid} ->
            new_state = put_in(state, [:subs, sub_id], {producer_pid, subscriber_pid})
            {Codec.encode_subscribe_ack(sub_id, Frame.err_none()), new_state}

          {:error, _reason} ->
            {Codec.encode_subscribe_ack(sub_id, Frame.err_invalid_request()), state}
        end

      {:error, _} ->
        {Codec.encode_error(0, Frame.err_invalid_request(), "bad subscribe payload"), state}
    end
  end

  # UNSUBSCRIBE (0x0D)
  defp handle_frame(0x0D, payload, state) do
    case Codec.decode_unsubscribe(payload) do
      {:ok, %{sub_id: sub_id}} ->
        case Map.get(state.subs, sub_id) do
          nil ->
            {Codec.encode_error(sub_id, Frame.err_invalid_request(), "unknown subscription"), state}

          {producer_pid, subscriber_pid} ->
            SubscriptionSupervisor.stop_subscription(producer_pid, subscriber_pid)
            {nil, %{state | subs: Map.delete(state.subs, sub_id)}}
        end

      {:error, _} ->
        {Codec.encode_error(0, Frame.err_invalid_request(), "bad unsubscribe payload"), state}
    end
  end

  defp handle_frame(_type, _payload, state) do
    {Codec.encode_error(0, Frame.err_invalid_request(), "unsupported frame type"), state}
  end

  # ---------------------------------------------------------------------------
  # Partition helpers
  # ---------------------------------------------------------------------------

  defp ensure_partition(topic, partition) do
    case get_partition(topic, partition) do
      nil ->
        TopicSupervisor.ensure_partition(topic, partition)
        get_partition(topic, partition)

      pid ->
        pid
    end
  end

  defp get_partition(topic, partition) do
    case Registry.lookup(Broker.Registry, {topic, partition}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp append_records(_pid, []), do: {:ok, -1}

  defp append_records(pid, records) do
    Enum.reduce_while(records, {:ok, 0}, fn %{key: k, value: v}, _acc ->
      case Partition.append(pid, k, v) do
        {:ok, offset} -> {:cont, {:ok, offset}}
        err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, last_offset} ->
        base = last_offset - length(records) + 1
        {:ok, max(base, 0)}

      err ->
        err
    end
  end
end
