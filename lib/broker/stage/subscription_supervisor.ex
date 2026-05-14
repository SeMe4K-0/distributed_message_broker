defmodule Broker.Stage.SubscriptionSupervisor do
  @moduledoc false
  use DynamicSupervisor

  require Logger

  alias Broker.Cluster.Manager
  alias Broker.Stage.{PartitionProducer, Subscriber}

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_subscription(String.t(), non_neg_integer(), integer(), pos_integer(), integer(), port()) ::
          {:ok, pid(), pid()} | {:error, term()}
  def start_subscription(topic, partition, start_offset, max_in_flight, sub_id, socket) do
    owner = owner_node(topic, partition)

    producer_result =
      if owner == node() do
        DynamicSupervisor.start_child(__MODULE__, {PartitionProducer, {topic, partition, start_offset}})
      else
        try do
          :erpc.call(owner, __MODULE__, :start_remote_producer, [topic, partition, start_offset])
        catch
          :error, {:erpc, reason} ->
            Logger.error("[SubscriptionSupervisor] Remote producer failed: #{inspect(reason)}, falling back to local")
            DynamicSupervisor.start_child(__MODULE__, {PartitionProducer, {topic, partition, start_offset}})
        end
      end

    with {:ok, producer_pid} <- producer_result,
         subscriber_spec = {Subscriber, {producer_pid, max_in_flight, sub_id, socket}},
         {:ok, subscriber_pid} <- DynamicSupervisor.start_child(__MODULE__, subscriber_spec) do
      {:ok, producer_pid, subscriber_pid}
    end
  end

  # Called via :erpc on the owning node to start a producer there.
  @spec start_remote_producer(String.t(), non_neg_integer(), integer()) ::
          {:ok, pid()} | {:error, term()}
  def start_remote_producer(topic, partition, start_offset) do
    DynamicSupervisor.start_child(__MODULE__, {PartitionProducer, {topic, partition, start_offset}})
  end

  @spec stop_subscription(pid(), pid()) :: :ok
  def stop_subscription(producer_pid, subscriber_pid) do
    DynamicSupervisor.terminate_child(__MODULE__, subscriber_pid)

    if node(producer_pid) == node() do
      DynamicSupervisor.terminate_child(__MODULE__, producer_pid)
    else
      try do
        :erpc.call(node(producer_pid), DynamicSupervisor, :terminate_child,
                   [__MODULE__, producer_pid])
      catch
        :error, _ -> :ok
      end
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp owner_node(topic, partition) do
    case Process.whereis(Manager) do
      nil -> node()
      _ -> Manager.node_for(topic, partition)
    end
  end
end
