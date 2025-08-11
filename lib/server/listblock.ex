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
    # Generate a globally monotonic order for FIFO across concurrent callers
    order = :erlang.unique_integer([:monotonic, :positive])

    Agent.update(__MODULE__, fn state ->
      Enum.reduce(keys, state, fn key, acc ->
        waiters = Map.get(acc, key, [])
        waiter = %{pid: pid, client: client, ref: ref, keys: keys, order: order}
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
        [] ->
          {:empty, state}

        waiters ->
          # Choose the earliest waiter by monotonic order to preserve FIFO
          earliest = Enum.min_by(waiters, & &1.order)
          remaining = Enum.reject(waiters, fn w -> w.ref == earliest.ref end)
          {{:ok, earliest}, Map.put(state, key, remaining)}
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

  @doc """
  Re-add a waiter (previously taken) back to all its keys.
  Note: this appends the waiter, not restoring exact original position.
  """
  def readd_waiter(%{keys: keys, pid: pid, client: client, ref: ref, order: order}) do
    Agent.update(__MODULE__, fn state ->
      Enum.reduce(keys, state, fn key, acc ->
        waiters = Map.get(acc, key, [])
        waiter = %{pid: pid, client: client, ref: ref, keys: keys, order: order}
        Map.put(acc, key, waiters ++ [waiter])
      end)
    end)
  end
end
