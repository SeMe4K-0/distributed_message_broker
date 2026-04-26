defmodule Broker.Topic.TopicSupervisor do
  @moduledoc false
  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec ensure_partition(String.t(), non_neg_integer()) :: {:ok, pid()} | {:error, term()}
  def ensure_partition(topic, partition) do
    opts = [data_dir: Application.get_env(:broker, :data_dir, "data")]
    child_spec = {Broker.Topic.PartitionSupervisor, {topic, partition, opts}}

    case DynamicSupervisor.start_child(__MODULE__, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      err -> err
    end
  end
end
