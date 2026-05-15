defmodule Broker.Network.Connection do
  @moduledoc false
  use GenServer, restart: :temporary

  require Logger

  alias Broker.Cluster.{Manager, RPC}
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
  # Buffer processing
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
  # Frame handlers
  # ---------------------------------------------------------------------------

  # PRODUCE (0x01) — route to owning node, retry on not_leader hint from Raft.
  defp handle_frame(0x01, payload, state) do
    response =
      case Codec.decode_produce(payload) do
        {:ok, %{topic: topic, partition: p, correlation_id: cid, records: records}} ->
          owner = owner_node(topic, p)
          result = produce_routed(owner, topic, p, records)

          case result do
            {:ok, base_offset} -> Codec.encode_produce_ack(cid, base_offset, Frame.err_none())
            {:error, _} -> Codec.encode_error(cid, Frame.err_invalid_request(), "append failed")
          end

        {:error, _} ->
          Codec.encode_error(0, Frame.err_invalid_request(), "bad produce payload")
      end

    {response, state}
  end

  # FETCH (0x03) — route to owning node
  defp handle_frame(0x03, payload, state) do
    response =
      case Codec.decode_fetch(payload) do
        {:ok,
         %{
           topic: topic,
           partition: p,
           offset: offset,
           max_bytes: max_bytes,
           correlation_id: cid
         }} ->
          owner = owner_node(topic, p)

          result =
            if owner == node() do
              case get_partition(topic, p) do
                nil ->
                  {:error, :unknown}

                pid ->
                  {:ok, records} = Partition.fetch(pid, offset, max_bytes)
                  hwm = Partition.high_watermark(pid)
                  {:ok, records, hwm}
              end
            else
              try do
                :erpc.call(owner, RPC, :fetch_with_hwm, [topic, p, offset, max_bytes])
              catch
                :error, {:erpc, reason} ->
                  Logger.error("[Connection] RPC fetch failed: #{inspect(reason)}")
                  {:ok, [], 0}
              end
            end

          case result do
            {:ok, records, hwm} ->
              Codec.encode_fetch_response(cid, hwm, records)

            {:error, :unknown} ->
              Codec.encode_error(cid, Frame.err_unknown_topic(), "unknown topic/partition")
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
            {Codec.encode_error(sub_id, Frame.err_invalid_request(), "unknown subscription"),
             state}

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
  # Routing helpers
  # ---------------------------------------------------------------------------

  defp owner_node(topic, partition) do
    case Process.whereis(Manager) do
      nil -> node()
      _ -> Manager.node_for(topic, partition)
    end
  end

  # Try the primary; if it tells us it's not the leader, retry on the indicated leader.
  defp produce_routed(target, topic, p, records, retried? \\ false) do
    result =
      if target == node() do
        pid = ensure_partition(topic, p)
        append_records(pid, records)
      else
        try do
          :erpc.call(target, RPC, :produce, [topic, p, records])
        catch
          :error, {:erpc, reason} ->
            Logger.error("[Connection] RPC produce failed: #{inspect(reason)}")
            {:error, :rpc_failed}
        end
      end

    case result do
      {:error, {:not_leader, leader}}
      when not retried? and not is_nil(leader) and leader != target ->
        produce_routed(leader, topic, p, records, true)

      other ->
        other
    end
  end

  # ---------------------------------------------------------------------------
  # Partition helpers (local-only)
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
        {:ok, max(last_offset - length(records) + 1, 0)}

      err ->
        err
    end
  end
end
