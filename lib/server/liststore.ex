defmodule Server.ListStore do
  use Agent

  @doc """
  Starts the ListStore Agent which holds a map of key => list(values).
  """
  def start_link(_args \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc """
  Append `element` to the list at `key`.

  If the list does not exist, it is created. Returns the length of the list
  after the push, as per Redis RPUSH semantics.
  """
  def rpush(key, element), do: rpush_many(key, [element])

  @doc """
  Append multiple `elements` to the list at `key` in order.

  If the list does not exist, it is created with the given elements. Returns
  the length of the list after the push.
  """
  def rpush_many(key, elements) when is_list(elements) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil ->
          new_list = elements
          {length(new_list), Map.put(state, key, new_list)}

        list when is_list(list) ->
          updated = list ++ elements
          {length(updated), Map.put(state, key, updated)}

        _other ->
          # Wrong type; reset to list for this stage's scope
          new_list = elements
          {length(new_list), Map.put(state, key, new_list)}
      end
    end)
  end

  @doc """
  Retrieve the list for a given key or nil if not present.
  """
  def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))
end
