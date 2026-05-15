defmodule Broker.Raft.Supervisor do
  @moduledoc false
  use DynamicSupervisor

  alias Broker.Raft.Server

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_server(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_server(opts) do
    DynamicSupervisor.start_child(__MODULE__, {Server, opts})
  end
end
