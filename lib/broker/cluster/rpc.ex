defmodule Broker.Cluster.RPC do
  @moduledoc false

  # Functions in this module are meant to be called via :erpc.call/4 from
  # other nodes in the cluster. They run on the *local* node (the owning node).

  alias Broker.Topic.{Partition, TopicSupervisor}

  @doc "Produce records to a locally-owned partition. Called via :erpc."
  @spec produce(String.t(), non_neg_integer(), [map()]) :: {:ok, integer()} | {:error, term()}
  def produce(topic, partition, records) do
    pid = ensure_partition(topic, partition)
    append_records(pid, records)
  end

  @doc "Fetch records + high-watermark from a locally-owned partition. Called via :erpc."
  @spec fetch_with_hwm(String.t(), non_neg_integer(), integer(), integer()) ::
          {:ok, [map()], integer()}
  def fetch_with_hwm(topic, partition, offset, max_bytes) do
    case get_partition(topic, partition) do
      nil ->
        {:ok, [], 0}

      pid ->
        {:ok, records} = Partition.fetch(pid, offset, max_bytes)
        hwm = Partition.high_watermark(pid)
        {:ok, records, hwm}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers (duplicated from Connection — kept small on purpose)
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
      {:ok, last_offset} -> {:ok, max(last_offset - length(records) + 1, 0)}
      err -> err
    end
  end
end
