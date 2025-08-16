defmodule Server.SortedSetStore do
  @moduledoc """
  Store for managing Redis Sorted Sets (ZSETs).

  A sorted set is a collection of unique elements where each element is associated
  with a floating-point score. Elements are ordered by their scores in ascending order.

  Implementation uses a combination of:
  - A map to store member -> score mappings for O(1) membership checks
  - A sorted list (using Enum.sort) to maintain order by score
  """

  use Agent

  @doc """
  Starts the SortedSetStore Agent.

  State structure:
  %{
    "sorted_set_key" => %{
      members: %{"member1" => 1.0, "member2" => 2.0},
      sorted_list: [{"member1", 1.0}, {"member2", 2.0}]
    }
  }
  """
  def start_link(_args \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc """
  Adds a member with a score to a sorted set.

  Returns the number of new members added (0 if member already exists, 1 if new).
  """
  def zadd(key, score, member) when is_number(score) and is_binary(member) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil ->
          # Create new sorted set
          new_zset = %{
            members: %{member => score},
            sorted_list: [{member, score}]
          }

          {1, Map.put(state, key, new_zset)}

        %{members: members, sorted_list: sorted_list} ->
          case Map.get(members, member) do
            nil ->
              # New member
              new_members = Map.put(members, member, score)
              new_sorted_list = insert_sorted({member, score}, sorted_list)
              new_zset = %{members: new_members, sorted_list: new_sorted_list}
              {1, Map.put(state, key, new_zset)}

            _existing_score ->
              # Member already exists, update score
              new_members = Map.put(members, member, score)
              # Remove old entry and insert new one
              filtered_list = Enum.reject(sorted_list, fn {m, _s} -> m == member end)
              new_sorted_list = insert_sorted({member, score}, filtered_list)
              new_zset = %{members: new_members, sorted_list: new_sorted_list}
              {0, Map.put(state, key, new_zset)}
          end
      end
    end)
  end

  @doc """
  Gets the sorted set for the given key.
  Returns nil if the key doesn't exist.
  """
  def get_sorted_set(key) do
    Agent.get(__MODULE__, fn state ->
      Map.get(state, key)
    end)
  end

  @doc """
  Gets the score for a member in a sorted set.
  Returns nil if key or member doesn't exist.
  """
  def zscore(key, member) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil -> nil
        %{members: members} -> Map.get(members, member)
      end
    end)
  end

  @doc """
  Gets the number of members in a sorted set.
  Returns 0 if the key doesn't exist.
  """
  def zcard(key) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil -> 0
        %{members: members} -> map_size(members)
      end
    end)
  end

  @doc """
  Gets the rank (0-based index) of a member in a sorted set.

  Returns the rank as an integer if the member exists, or nil if either
  the key or member doesn't exist.

  Members are ordered by score (ascending), and for members with the same score,
  they are ordered lexicographically.
  """
  def zrank(key, member) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key) do
        nil ->
          nil

        %{members: members, sorted_list: sorted_list} ->
          case Map.get(members, member) do
            nil ->
              nil

            _score ->
              # Find the index of the member in the sorted list
              find_member_index(sorted_list, member, 0)
          end
      end
    end)
  end

  # Private helper to find the index of a member in the sorted list
  defp find_member_index([], _member, _index) do
    # Member not found (shouldn't happen if called correctly)
    nil
  end

  defp find_member_index([{member, _score} | _rest], member, index) do
    # Found the member at this index
    index
  end

  defp find_member_index([{_other_member, _score} | rest], member, index) do
    # Continue searching
    find_member_index(rest, member, index + 1)
  end

  # Private helper to insert an element in the correct position in a sorted list
  # Maintains order by score (ascending), then by member name (lexicographically)
  defp insert_sorted({member, score}, sorted_list) do
    insert_sorted_helper({member, score}, sorted_list, [])
  end

  defp insert_sorted_helper({member, score}, [], acc) do
    Enum.reverse([{member, score} | acc])
  end

  defp insert_sorted_helper({member, score}, [{m, s} | rest], acc) do
    cond do
      score < s ->
        # Insert here
        Enum.reverse(acc) ++ [{member, score}, {m, s}] ++ rest

      score == s and member <= m ->
        # Same score, insert by lexicographical order
        Enum.reverse(acc) ++ [{member, score}, {m, s}] ++ rest

      true ->
        # Continue searching
        insert_sorted_helper({member, score}, rest, [{m, s} | acc])
    end
  end
end
