defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """
require Logger

  use Application

  def start(_type, _args) do
    config = parse_args()

    children = [
      Server.Store,
      Server.Replicationstate,
      Server.Commandbuffer,
      {Task, fn -> Server.listen(config) end}
    ]

    opts = [strategy: :one_for_one, name: :sup]
    Supervisor.start_link(children, opts)
  end

  defp parse_args do
    {opts, _, _} = OptionParser.parse(System.argv(),
      switches: [port: :integer, replicaof: :string])

    port = opts[:port] || 6379
    replica_of = parse_replicaof(opts[:replicaof])

    %{port: port, replica_of: replica_of}
  end

  defp parse_replicaof(nil), do: nil
  defp parse_replicaof(replicaof) do
    [host, port] = String.split(replicaof, " ")
    {host, String.to_integer(port)}
  end

  @doc """
  Listen for incoming connections
  """
  def listen(config) do
    IO.puts("Server listening on port 6379")
    {:ok, socket} = :gen_tcp.listen(config.port, [:binary, active: false, reuseaddr: true])

    if config.replica_of do
      connect_to_master(config.replica_of, config.port)
    end

    loop_acceptor(socket, config)
  end

  defp connect_to_master({master_host, master_port}, replica_port) do
    case :gen_tcp.connect(to_charlist(master_host), master_port, [:binary, active: false]) do
      {:ok, socket} ->
        perfrom_handshake(socket, replica_port)
      {:error, reason} ->
        IO.puts("Failed to connect to master: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp perfrom_handshake(socket, replica_port) do
    with :ok <- send_ping(socket),
         :ok <- send_replconf_listening_port(socket, replica_port),
         :ok <- send_replconf_capa(socket),
         :ok <- send_psync(socket) do

      :ok
    else
      {:error, reason} ->
        IO.puts("Handshake failed: #{inspect(reason)}")
        :gen_tcp.close(socket)
        {:error, reason}
    end
  end

  defp send_ping(socket) do
    send_command(socket, ["PING"], "+PONG\r\n")
  end

  defp send_replconf_listening_port(socket, port) do
    send_command(socket, ["REPLCONF", "listening-port", to_string(port)], "+OK\r\n")
  end

  defp send_replconf_capa(socket) do
    send_command(socket, ["REPLCONF", "capa", "psync2"], "+OK\r\n")
  end

  defp send_psync(socket) do
    send_command(socket, ["PSYNC", "?", "-1"], "+FULLRESYNC <REPL_ID> 0\r\n")
  end

  defp send_command(socket, command, expected_response) do
    packed_command = Server.Protocol.pack(command) |> IO.iodata_to_binary()
    case :gen_tcp.send(socket, packed_command) do
      :ok ->
        receive_response(socket, expected_response)
      {:error, reason} ->
        IO.puts("Failed to send command: #{inspect(command)}")
        {:error, reason}
    end
  end

  defp receive_response(socket, expected_response) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, ^expected_response} ->
        :ok
      {:ok, response} ->
        IO.puts("Unexpected response: #{inspect(response)}")
        {:error, :unexpected_response}
      {:error, reason} ->
        IO.puts("Error receiving response: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Master Server Code
  defp loop_acceptor(socket, config) do
    {:ok, client} = :gen_tcp.accept(socket)
    spawn(fn -> serve(client, config) end)
    loop_acceptor(socket, config)
  end

  defp serve(client, config) do
      client
      |> read_line
      |> process_command(client, config)

      serve(client, config)
  end

  defp read_line(client) do
    {:ok, data} = :gen_tcp.recv(client, 0)
    data
  end

  defp process_command(command, client, config) do
    IO.puts("Received command: #{inspect(command)}")  # Debug line
    case Server.Protocol.parse(command) do
      {:ok, parsed_data, _rest} ->
        IO.puts("Parsed data: #{inspect(parsed_data)}")  # Debug line
        handle_command(parsed_data, client, config)
      {:continuation, _fun} ->
        IO.puts("Incomplete command")  # Debug line
        write_line("-ERR Incomplete command\r\n", client)
    end
  end

  defp handle_command(parsed_data, client, config) do
    case parsed_data do
      [command | args] ->
        execute_command_with_config(String.upcase(to_string(command)), args, client, config)
      _ ->
        write_line("-ERR Invalid command format\r\n", client)
    end
  end

  # ---------------------------------------------------------------
  # Helpers
  defp replication_id do
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
  end

  defp replication_offset do
    0
  end

  defp empty_rdb_file do
    content = Base.decode64!("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
    IO.puts("RDB file size: #{byte_size(content)} bytes")
    IO.puts("RDB file header: #{inspect(binary_part(content, 0, 9))}")
    content
  end


  defp send_buffered_commands_to_replica do
    commands = Server.Commandbuffer.get_and_clear_commands()
    case Server.Replicationstate.get_replica_socket() do
      nil ->
        IO.puts("NO replica found")
        :ok  # No replica connected
      socket ->
        Enum.each(commands, fn command ->
          packed_command = Server.Protocol.pack(command) |> IO.iodata_to_binary()
          :gen_tcp.send(socket, packed_command)
        end)
    end
  end
  # -------------------------------------------------------------------

  defp execute_command_with_config(command, args, client, config) do
    case command do
      "INFO" when args == ["replication"] ->
        handle_info_replication(client, config)
      _ ->
        execute_command(command, args, client)
    end
  end

  defp handle_info_replication(client, config) do

    response = case config.replica_of do
      nil ->
        """
        role:master
        master_replid:#{replication_id()}
        master_repl_offset:#{replication_offset()}
        """
      {_, _} ->
        "role:slave"
    end

    packed_response = Server.Protocol.pack(response) |> IO.iodata_to_binary()
    write_line(packed_response, client)
  end

  defp execute_command("REPLCONF", args, client) do
    IO.puts("Received REPLCONF with args: #{inspect(args)}")

    message = "+OK\r\n"
    write_line(message, client)
  end


  defp execute_command("PSYNC", _args, client) do
    try do
      response = "+FULLRESYNC #{replication_id()} #{replication_offset()}\r\n"
      :ok = :gen_tcp.send(client, response)
      send_rdb_file(client)
    catch
      :error, :closed ->
        Logger.warning("Connection closed while executing PSYNC")
      error ->
        Logger.error("Error executing PSYNC: #{inspect(error)}")
    end
  end

  defp send_rdb_file(client) do
    try do
      rdb_content = empty_rdb_file()
      length = byte_size(rdb_content)
      header = "$#{length}\r\n"
      :ok = :gen_tcp.send(client, [header, rdb_content])
      send_buffered_commands_to_replica()
    catch
      :error, :closed ->
        Logger.warning("Connection closed while sending RDB file")
      error ->
        Logger.error("Error sending RDB file: #{inspect(error)}")
    end
  end


  defp execute_command("ECHO", [message], client) do
    response = Server.Protocol.pack(message) |> IO.iodata_to_binary()
    write_line(response, client)
  end

  defp execute_command("SET", [key, value | rest], client) do
    try do
      case rest do
        ["PX", time] ->
          time_ms = String.to_integer(time)
          Server.Store.update(key, value, time_ms)
        [] ->
          Server.Store.update(key, value)
      end

      write_line("+OK\r\n", client)
      Server.Commandbuffer.add_command(["SET", key, value | rest])
      :ok
    catch
      _ ->
        write_line("-ERR Internal server error\r\n", client)
    end
  end

  defp execute_command("GET", [key], client) do
    case Server.Store.get_value_or_false(key) do
      {:ok, value} ->
        response = Server.Protocol.pack(value) |> IO.iodata_to_binary()
        write_line(response, client)

      {:error, _reason} ->
        write_line("$-1\r\n", client)
    end
  end

  defp execute_command("PING", [], client) do
    write_line("+PONG\r\n", client)
  end

  defp execute_command(command, _args, client) do
    write_line("-ERR Unknown command '#{command}'\r\n", client)
  end

  defp write_line(line, client) do
    :gen_tcp.send(client, line)
  end


end
