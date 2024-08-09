defmodule Server.ClientState do
  use Agent

  def start_link(_) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def start_transaction(client) do
    Agent.update(__MODULE__, fn state ->
      Map.put(state, client, %{in_transaction: true, queued_commands: []})
    end)
  end

  def end_transaction(client) do
    Agent.update(__MODULE__, fn state ->
      Map.delete(state, client)
    end)
  end

  def in_transaction?(client) do
    Agent.get(__MODULE__, fn state ->
      case Map.get(state, client) do
        nil -> false
        %{in_transaction: in_transaction} -> in_transaction
      end
    end)
  end
end
