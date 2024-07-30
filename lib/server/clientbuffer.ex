defmodule Server.Clientbuffer do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    {:ok, []}
  end

  def add_client(client) do
    GenServer.cast(__MODULE__, {:add_client, client})
  end

  def get_clients do
    GenServer.call(__MODULE__, :get_clients)
  end

  def handle_cast({:add_client, client}, clients) do
    {:noreply, [client | clients]}
  end

  def handle_call(:get_clients, _from, clients) do
    {:reply, clients, clients}
  end
end
