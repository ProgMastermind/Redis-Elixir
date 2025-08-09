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
  Prepend `element` to the list at `key`.

  If the list does not exist, it is created. Returns the length of the list
  after the push, as per Redis LPUSH semantics.
  """
  def lpush(key, element), do: lpush_many(key, [element])

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
  Prepend multiple `elements` to the list at `key`.

  When multiple elements are provided, they are pushed from left to right,
  resulting in the last provided element being at the leftmost position, e.g.,
  LPUSH key "a" "b" "c" => ["c", "b", "a", ...].
  Returns the length of the list after the push.
  """
  def lpush_many(key, elements) when is_list(elements) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil ->
          new_list = Enum.reverse(elements)
          {length(new_list), Map.put(state, key, new_list)}

        list when is_list(list) ->
          updated = Enum.reverse(elements) ++ list
          {length(updated), Map.put(state, key, updated)}

        _other ->
          # Wrong type; reset to list for this stage's scope
          new_list = Enum.reverse(elements)
          {length(new_list), Map.put(state, key, new_list)}
      end
    end)
  end

  @doc """
  Retrieve the list for a given key or nil if not present.
  """
  def get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))

  @doc """
  Return the length of the list stored at `key`.

  If the list does not exist, returns 0.
  """
  def llen(key) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key) do
        list when is_list(list) -> length(list)
        _ -> 0
      end
    end)
  end

  @doc """
  Remove and return the first element from the list at `key`.

  Returns `{:ok, value}` if an element was popped, or `:empty` if the list
  is empty or does not exist.
  """
  def lpop(key) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, key) do
        [head | tail] ->
          {{:ok, head}, Map.put(state, key, tail)}

        [] ->
          {:empty, Map.put(state, key, [])}

        nil ->
          {:empty, state}

        _other ->
          # For non-list types (out of scope for strict type errors in this stage)
          {:empty, state}
      end
    end)
  end

  @doc """
  Return a sublist from start to stop (inclusive) using 0-based indices.

  Semantics (aligned with the user's LRANGE spec):
  - If the list does not exist, returns []
  - If start >= list length, returns []
  - If stop >= list length, stop is clamped to last index
  - If start > stop after clamping, returns []
  """
  def lrange(key, start_index, stop_index)
      when is_integer(start_index) and is_integer(stop_index) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key) do
        list when is_list(list) ->
          len = length(list)

          if len == 0 do
            []
          else
            start_pos = normalize_index(start_index, len)
            stop_pos = normalize_index(stop_index, len) |> min(len - 1)

            cond do
              start_pos >= len -> []
              start_pos > stop_pos -> []
              true -> take_slice(list, start_pos, stop_pos)
            end
          end

        _ ->
          []
      end
    end)
  end

  defp take_slice(_list, start_idx, stop_idx) when stop_idx < start_idx, do: []
  defp take_slice(list, start_idx, stop_idx), do: Enum.slice(list, start_idx..stop_idx)

  defp normalize_index(index, _len) when index >= 0, do: index

  defp normalize_index(index, len) do
    pos = len + index
    if pos < 0, do: 0, else: pos
  end
end
