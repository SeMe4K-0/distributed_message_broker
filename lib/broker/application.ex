defmodule Broker.Application do
  @moduledoc false
  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Broker.Registry},
      Broker.Topic.TopicSupervisor,
      {DynamicSupervisor, name: Broker.ConnectionSupervisor, strategy: :one_for_one},
      {Broker.Network.Listener, port: Application.get_env(:broker, :port, 9092)}
    ]

    opts = [strategy: :one_for_one, name: Broker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
