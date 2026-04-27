defmodule Broker.Topic.PartitionSupervisor do
  @moduledoc false
  use Supervisor

  def start_link({topic, partition, opts}) do
    Supervisor.start_link(__MODULE__, {topic, partition, opts})
  end

  @impl Supervisor
  def init({topic, partition, opts}) do
    data_dir = Keyword.get(opts, :data_dir, Application.get_env(:broker, :data_dir, "data"))
    dir = Path.join([data_dir, topic, "#{partition}"])
    File.mkdir_p!(dir)

    children = [
      {Broker.Log.SegmentManager, {topic, partition, opts}},
      {Broker.Log.Compactor, [dir: dir, retention_ms: Keyword.get(opts, :retention_ms, 7 * 24 * 60 * 60 * 1000)]},
      {Broker.Topic.Partition, {topic, partition, opts}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
