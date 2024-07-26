defmodule Server.Store do
  use Agent

  def start_link do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def update(key, value) do
    Agent.update(__MODULE__, &Map.put(&1, key, value))
  end

  def get_value_or_false(key) do
    Agent.get(__MODULE__, fn map ->
      case Map.fetch(map, key) do
        {:ok, value} -> {:ok, value}
        :error -> {:error, "key not found"}
      end
    end)
  end
end
