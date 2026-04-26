defmodule Broker.Topic.PartitionSupervisor do
  @moduledoc false
  use Supervisor

  def start_link({topic, partition, opts}) do
    Supervisor.start_link(__MODULE__, {topic, partition, opts})
  end

  @impl Supervisor
  def init({topic, partition, opts}) do
    children = [
      {Broker.Topic.Partition, {topic, partition, opts}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
