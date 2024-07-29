defmodule Server.Replicationstate do
  use Agent

  def start_link(_ \\ []) do
    Agent.start_link(fn -> %{socket: nil, handshake_complete: false} end, name: __MODULE__)
  end

  def set_replica_socket(socket) do
    Agent.update(__MODULE__, fn state -> %{state | socket: socket} end)
  end

  def get_replica_socket do
    Agent.get(__MODULE__, fn state -> state.socket end)
  end

  def set_handshake_complete do
    Agent.update(__MODULE__, fn state -> %{state | handshake_complete: true} end)
  end

  def handshake_complete? do
    Agent.get(__MODULE__, fn state -> state.handshake_complete end)
  end
end
