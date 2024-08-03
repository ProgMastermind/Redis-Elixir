defmodule Server.Bytes do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, 0, name: __MODULE__)
  end

  def init(offset) do
    {:ok, offset}
  end

  def get_offset do
    GenServer.call(__MODULE__, :get_offset)
  end

  def increment_offset(bytes) do
    GenServer.cast(__MODULE__, {:increment, bytes})
  end

  def handle_call(:get_offset, _from, offset) do
    {:reply, offset, offset}
  end

  def handle_cast({:increment, bytes}, offset) do
    {:noreply, offset + bytes}
  end
end
