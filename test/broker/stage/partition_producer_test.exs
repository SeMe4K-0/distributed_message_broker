defmodule Broker.Stage.PartitionProducerTest do
  use ExUnit.Case, async: false

  alias Broker.Stage.PartitionProducer
  alias Broker.Topic.{Partition, TopicSupervisor}

  @port 19094

  setup_all do
    dir = System.tmp_dir!() |> Path.join("producer_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(dir)

    Application.stop(:broker)
    Application.put_env(:broker, :port, @port)
    Application.put_env(:broker, :data_dir, dir)
    {:ok, _} = Application.ensure_all_started(:broker)

    on_exit(fn ->
      Application.stop(:broker)
      File.rm_rf!(dir)
    end)

    :ok
  end

  defp ensure_partition(topic) do
    TopicSupervisor.ensure_partition(topic, 0)
    Partition.via(topic, 0)
  end

  test "delivers all existing records on demand" do
    pid = ensure_partition("pp_emit")
    for i <- 1..5, do: Partition.append(pid, "k#{i}", "v#{i}")

    {:ok, producer} = PartitionProducer.start_link({"pp_emit", 0, 0})
    {:ok, store} = start_collector(producer, max_demand: 10)

    Process.sleep(300)

    collected = Agent.get(store, & &1)
    assert length(collected) == 5
    assert Enum.map(collected, & &1.key) == ~w[k1 k2 k3 k4 k5]
  end

  test "tail-follows new records after catching up" do
    pid = ensure_partition("pp_tail")

    {:ok, producer} = PartitionProducer.start_link({"pp_tail", 0, 0})
    {:ok, store} = start_collector(producer, max_demand: 10)

    Process.sleep(50)
    assert Agent.get(store, & &1) == []

    Partition.append(pid, "ka", "va")
    Partition.append(pid, "kb", "vb")
    Process.sleep(400)

    collected = Agent.get(store, & &1)
    assert length(collected) == 2
    assert Enum.map(collected, & &1.key) == ["ka", "kb"]
  end

  test "respects start_offset — skips earlier records" do
    pid = ensure_partition("pp_offset")
    for i <- 0..4, do: Partition.append(pid, "k#{i}", "v#{i}")

    # Start from offset 3 — should only deliver k3, k4
    {:ok, producer} = PartitionProducer.start_link({"pp_offset", 0, 3})
    {:ok, store} = start_collector(producer, max_demand: 10)

    Process.sleep(300)

    collected = Agent.get(store, & &1)
    assert length(collected) == 2
    assert Enum.map(collected, & &1.key) == ["k3", "k4"]
  end

  test "max_demand limits events in-flight per batch" do
    pid = ensure_partition("pp_batch")
    for i <- 1..9, do: Partition.append(pid, "k#{i}", "v#{i}")

    # max_demand: 3 means at most 3 events delivered per dispatch
    {:ok, producer} = PartitionProducer.start_link({"pp_batch", 0, 0})
    {:ok, store, batch_sizes} = start_collector_with_batches(producer, max_demand: 3)

    Process.sleep(400)

    collected = Agent.get(store, & &1)
    assert length(collected) == 9

    # Each individual batch delivered to handle_events must be <= max_demand
    for size <- Agent.get(batch_sizes, & &1) do
      assert size <= 3
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp start_collector(producer_pid, opts) do
    {:ok, store} = Agent.start_link(fn -> [] end)
    max_demand = Keyword.get(opts, :max_demand, 10)
    {:ok, _} = GenStage.start_link(__MODULE__.Collector, {producer_pid, max_demand, store, nil})
    {:ok, store}
  end

  defp start_collector_with_batches(producer_pid, opts) do
    {:ok, store} = Agent.start_link(fn -> [] end)
    {:ok, batches} = Agent.start_link(fn -> [] end)
    max_demand = Keyword.get(opts, :max_demand, 10)
    {:ok, _} = GenStage.start_link(__MODULE__.Collector, {producer_pid, max_demand, store, batches})
    {:ok, store, batches}
  end

  defmodule Collector do
    use GenStage

    def init({producer_pid, max_demand, store, batches}) do
      {:consumer, {store, batches},
       subscribe_to: [{producer_pid, max_demand: max_demand, min_demand: 0}]}
    end

    def handle_events(events, _from, {store, batches} = state) do
      Agent.update(store, &(&1 ++ events))
      if batches, do: Agent.update(batches, &[length(events) | &1])
      {:noreply, [], state}
    end
  end
end
