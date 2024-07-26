defmodule Server.Store do
  use Agent

  def start_link(_args \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def update(key, value, ttl \\ nil) do
    expiry = case ttl do
      nil -> nil
      ttl when is_integer(ttl) and ttl > 0 ->
        :os.system_time(:millisecond) + ttl
      _ ->
        nil
    end
    Agent.update(__MODULE__, &Map.put(&1, key, {value, expiry}))
  end

  def get_value_or_false(key) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil -> {:error, :not_found}
        {value, nil} -> {:ok, value}
        {value, expiry} ->
          if expiry > :os.system_time(:millisecond) do
            {:ok, value}
          else
            Agent.update(__MODULE__, &Map.delete(&1, key))
            {:error, :not_found}
          end
      end
    end)
  end
end
