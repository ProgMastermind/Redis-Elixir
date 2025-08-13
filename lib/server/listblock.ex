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
  Peek at the first waiter for a given key without removing it. Returns {:ok, waiter} | :empty.
  """
  def peek_first_waiter(key) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, key, []) do
        [] ->
          :empty

        waiters ->
          # Choose the earliest waiter by monotonic order to preserve FIFO
          earliest = Enum.min_by(waiters, & &1.order)
          {:ok, earliest}
      end
    end)
  end

  @doc """
  Atomically register as waiter and check if we're first. Returns {:first, ref} | {:not_first, ref}.
  """
  def register_and_check_first(keys, pid, client) do
    ref = make_ref()
    order = :erlang.unique_integer([:monotonic, :positive])

    Agent.get_and_update(__MODULE__, fn state ->
      # First, add ourselves to all keys
      new_state =
        Enum.reduce(keys, state, fn key, acc ->
          waiters = Map.get(acc, key, [])
          waiter = %{pid: pid, client: client, ref: ref, keys: keys, order: order}
          Map.put(acc, key, waiters ++ [waiter])
        end)

      # Then check if we're first for any key
      is_first =
        Enum.any?(keys, fn key ->
          waiters = Map.get(new_state, key, [])

          case waiters do
            [] ->
              false

            _ ->
              earliest = Enum.min_by(waiters, & &1.order)
              earliest.ref == ref
          end
        end)

      result = if is_first, do: {:first, ref}, else: {:not_first, ref}
      {result, new_state}
    end)
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
          {flag or removed != [], Map.put(acc, key, kept)}
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

  # In Server.ListBlock

  @doc """
  Atomically tries to LPOP a value from any of the keys.
  If successful, returns {:ok, {key, value}}.
  If all lists are empty, registers a waiter and returns {:wait, ref}.
  """
  def pop_or_register_waiter(keys, pid, client) do
    ref = make_ref()
    order = :erlang.unique_integer([:monotonic, :positive])

    Agent.get_and_update(__MODULE__, fn state ->
      # First, try to pop a value from the actual data store
      pop_result =
        Enum.find_value(keys, fn key ->
          case Server.ListStore.lpop(key) do
            {:ok, value} -> {key, value}
            :empty -> nil
          end
        end)

      if pop_result do
        # We found a value! Return it and don't change the waiter state.
        {{:ok, pop_result}, state}
      else
        # No value found. Add ourselves as a waiter to all keys.
        new_state =
          Enum.reduce(keys, state, fn key, acc ->
            waiters = Map.get(acc, key, [])
            waiter = %{pid: pid, client: client, ref: ref, keys: keys, order: order}
            Map.put(acc, key, waiters ++ [waiter])
          end)

        {{:wait, ref}, new_state}
      end
    end)
  end
end
