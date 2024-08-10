defmodule Server.Streamstore do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state) do
    {:ok, state}
  end

  def add_entry(stream_key, id, entry) do
    GenServer.call(__MODULE__, {:add_entry, stream_key, id, entry})
  end

  def get_stream(stream_key) do
    GenServer.call(__MODULE__, {:get_stream, stream_key})
  end

  def handle_call({:add_entry, stream_key, id, entry}, _from, state) do
    new_state = Map.update(state, stream_key, [{id, entry}], fn entries ->
      [{id, entry} | entries]
    end)
    {:reply, id, new_state}
  end

  def handle_call({:get_stream, stream_key}, _from, state) do
    {:reply, Map.get(state, stream_key), state}
  end
end
