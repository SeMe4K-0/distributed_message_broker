defmodule Broker.Stage.SubscriptionSupervisor do
  @moduledoc false
  use DynamicSupervisor

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
    producer_spec = {PartitionProducer, {topic, partition, start_offset}}

    with {:ok, producer_pid} <- DynamicSupervisor.start_child(__MODULE__, producer_spec),
         subscriber_spec = {Subscriber, {producer_pid, max_in_flight, sub_id, socket}},
         {:ok, subscriber_pid} <- DynamicSupervisor.start_child(__MODULE__, subscriber_spec) do
      {:ok, producer_pid, subscriber_pid}
    end
  end

  @spec stop_subscription(pid(), pid()) :: :ok
  def stop_subscription(producer_pid, subscriber_pid) do
    DynamicSupervisor.terminate_child(__MODULE__, subscriber_pid)
    DynamicSupervisor.terminate_child(__MODULE__, producer_pid)
    :ok
  end
end
