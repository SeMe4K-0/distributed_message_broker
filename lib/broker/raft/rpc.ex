defmodule Broker.Raft.RPC do
  @moduledoc false

  # Functions invoked via :erpc from peer nodes for Raft-driven partition operations.

  alias Broker.Raft.Server
  alias Broker.Topic.{Partition, TopicSupervisor}

  @doc "Propose a batch of records through the local Raft leader. Returns {:ok, base_offset} | {:error, ...}"
  @spec propose(String.t(), non_neg_integer(), [map()]) :: {:ok, integer()} | {:error, term()}
  def propose(_topic, _partition, []), do: {:ok, -1}

  def propose(topic, partition, records) do
    _ = ensure_partition(topic, partition)

    case propose_each(topic, partition, records, nil) do
      {:ok, last_offset} -> {:ok, max(last_offset - length(records) + 1, 0)}
      err -> err
    end
  end

  defp propose_each(_topic, _partition, [], last), do: {:ok, last}

  defp propose_each(topic, partition, [%{key: k, value: v} | rest], _last) do
    case Server.propose(Server.name(topic, partition), {:append, k, v}) do
      {:ok, {:ok, offset}} -> propose_each(topic, partition, rest, offset)
      {:ok, {:error, _} = err} -> err
      {:error, _} = err -> err
    end
  end

  @doc "Fetch records + high-watermark from the local replica's SegmentManager."
  @spec fetch_with_hwm(String.t(), non_neg_integer(), integer(), integer()) ::
          {:ok, [map()], integer()}
  def fetch_with_hwm(topic, partition, offset, max_bytes) do
    case lookup_partition(topic, partition) do
      nil ->
        {:ok, [], 0}

      pid ->
        {:ok, records} = Partition.fetch(pid, offset, max_bytes)
        hwm = Partition.high_watermark(pid)
        {:ok, records, hwm}
    end
  end

  # Ensure the partition exists locally (starts SegmentManager + Raft).
  defp ensure_partition(topic, partition) do
    case lookup_partition(topic, partition) do
      nil ->
        TopicSupervisor.ensure_partition(topic, partition)
        lookup_partition(topic, partition)

      pid ->
        pid
    end
  end

  defp lookup_partition(topic, partition) do
    case Registry.lookup(Broker.Registry, {topic, partition}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end
end
