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
    # Start the local PartitionSupervisor (idempotent).
    result = ensure_local(topic, partition)

    # Also ensure the partition exists on all replica nodes (so Raft has peers).
    ensure_on_replicas(topic, partition)

    result
  end

  @doc "Idempotently start the PartitionSupervisor on the local node only."
  @spec ensure_local(String.t(), non_neg_integer()) :: {:ok, pid()} | {:error, term()}
  def ensure_local(topic, partition) do
    opts = [data_dir: Application.get_env(:broker, :data_dir, "data")]
    child_spec = {Broker.Topic.PartitionSupervisor, {topic, partition, opts}}

    case DynamicSupervisor.start_child(__MODULE__, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      err -> err
    end
  end

  # Fire-and-forget :erpc calls to ensure each replica has its own PartitionSupervisor.
  # If the call fails (peer down), we ignore — Raft will deal with the missing replica.
  defp ensure_on_replicas(topic, partition) do
    case Process.whereis(Broker.Cluster.Manager) do
      nil ->
        :ok

      _ ->
        replicas = Broker.Cluster.Manager.replicas_for(topic, partition)
        others = Enum.reject(replicas, &(&1 == node()))

        for n <- others do
          try do
            :erpc.call(n, __MODULE__, :ensure_local, [topic, partition], 1000)
          catch
            :error, _ -> :ok
            :exit, _ -> :ok
          end
        end

        :ok
    end
  end
end
