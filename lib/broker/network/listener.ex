defmodule Broker.Network.Listener do
  @moduledoc false
  use GenServer, restart: :permanent

  require Logger

  @default_port 9092
  @accept_timeout 1_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    port = Keyword.get(opts, :port, @default_port)

    case :gen_tcp.listen(port, [
           :binary,
           packet: :raw,
           active: false,
           reuseaddr: true,
           backlog: 128
         ]) do
      {:ok, listen_socket} ->
        Logger.info("[Listener] Listening on port #{port}")
        send(self(), :accept)
        {:ok, %{listen_socket: listen_socket, port: port}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_info(:accept, %{listen_socket: lsock} = state) do
    case :gen_tcp.accept(lsock, @accept_timeout) do
      {:ok, client_socket} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            Broker.ConnectionSupervisor,
            {Broker.Network.Connection, {client_socket, []}}
          )

        :gen_tcp.controlling_process(client_socket, pid)

      {:error, :timeout} ->
        :ok

      {:error, :closed} ->
        Logger.warning("[Listener] Listen socket closed, stopping")
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.error("[Listener] Accept error: #{inspect(reason)}")
    end

    send(self(), :accept)
    {:noreply, state}
  end
end
