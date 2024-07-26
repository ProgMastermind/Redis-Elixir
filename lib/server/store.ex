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
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil -> {{:error, :not_found}, state}
        {value, nil} -> {{:ok, value}, state}
        {value, expiry} ->
          current_time = :os.system_time(:millisecond)
          if expiry > current_time do
            {{:ok, value}, state}
          else
            {{:error, :expired}, Map.delete(state, key)}
          end
      end
    end)
  end
end
