defmodule Server.ListBlock do
  use Agent

  @moduledoc """
  Manages BLPOP waiters per list key.

  State structure: %{key => [waiter, ...]}
  where waiter is a map with keys: :pid, :client, :ref, :keys
  """

  def start_link(_args \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc """
  Add a waiter to all provided keys. Returns the reference used.
  """
  def add_waiter(keys, pid, client, ref) when is_list(keys) do
    Agent.update(__MODULE__, fn state ->
      Enum.reduce(keys, state, fn key, acc ->
        waiters = Map.get(acc, key, [])
        waiter = %{pid: pid, client: client, ref: ref, keys: keys}
        Map.put(acc, key, waiters ++ [waiter])
      end)
    end)

    ref
  end

  @doc """
  Take the oldest waiter for a given key. Returns {:ok, waiter} | :empty.
  """
  def take_waiter(key) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, key, []) do
        [waiter | rest] ->
          {{:ok, waiter}, Map.put(state, key, rest)}

        [] ->
          {:empty, state}
      end
    end)
  end

  @doc """
  Remove a waiter by ref from all keys where it appears.
  """
  def remove_waiter_by_ref(ref) do
    Agent.get_and_update(__MODULE__, fn state ->
      {removed?, new_state} =
        Enum.reduce(state, {false, %{}}, fn {key, waiters}, {flag, acc} ->
          {kept, removed} = Enum.split_with(waiters, fn w -> w.ref != ref end)
          {flag or (removed != []), Map.put(acc, key, kept)}
        end)

      {removed?, new_state}
    end)
  end
end
