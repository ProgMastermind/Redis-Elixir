defmodule Server.Replicationstate do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_args \\ []) do
    {:ok, %{replica_socket: nil}}
  end

  def set_replica_socket(socket) do
    GenServer.call(__MODULE__, {:set_socket, socket})
  end

  def get_replica_socket do
    GenServer.call(__MODULE__, :get_socket)
  end

  def handle_call({:set_socket, socket}, _from, state) do
    {:reply, :ok, %{state | replica_socket: socket}}
  end

  def handle_call(:get_socket, _from, state) do
    {:reply, state.replica_socket, state}
  end
end
