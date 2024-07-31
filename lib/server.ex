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
      Server.Clientbuffer,
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
    IO.puts("Server listening on port #{config.port}")
    {:ok, socket} = :gen_tcp.listen(config.port, [:binary, active: false, reuseaddr: true])

    if config.replica_of do
      spawn_link(fn ->
         connect_to_master(config.replica_of, config.port)
      end)
    end

    loop_acceptor(socket, config)
  end

  defp connect_to_master({master_host, master_port}, replica_port) do
    case :gen_tcp.connect(to_charlist(master_host), master_port, [:binary, active: false]) do
      {:ok, socket} ->
        case perform_handshake(socket, replica_port) do
          :ok ->
            Logger.info("Handshake completed successfully")
            IO.puts("Process the received data carefully")
            case receive_rdb_file(socket) do
              # {:ok, :command, command} ->
              #   execute_set_command(command)
              #   listen_for_master_commands(socket)
              # :ok ->
              #   listen_for_master_commands(socket)
              {:ok, :commands, commands} ->
                Enum.each(commands, fn command ->
                  Logger.info("Executing command: #{inspect(command)}")
                  execute_set_command(command)
                end)
                listen_for_master_commands(socket)
              {:error, reason} ->
                Logger.error("Failed to receive RDB file: #{inspect(reason)}")
            end
          {:error, reason} ->
            Logger.error("Handshake failed: #{inspect(reason)}")
        end
      {:error, reason} ->
        Logger.error("Failed to connect to master: #{inspect(reason)}")
    end
  end

  defp listen_for_master_commands(socket) do
    Logger.info("Listening for commands from master")
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        process_master_command(data, socket)
        listen_for_master_commands(socket)
      {:error, :closed} ->
        Logger.info("Master connection closed")
      {:error, reason} ->
        Logger.error("Error receiving command from master: #{inspect(reason)}")
    end
  end

  defp process_master_command(data, socket) do
    Logger.debug("Received data from master: #{inspect(data)}")
    case Server.Protocol.parse(data) do
      {:ok, parsed_command, rest} ->
        Logger.info("Parsed command from master: #{inspect(parsed_command)}")
        execute_set_command(parsed_command)
        if rest != "", do: process_master_command(rest, socket)
      {:continuation, _} ->
        Logger.debug("Incomplete command, waiting for more data")
        case :gen_tcp.recv(socket, 0) do
          {:ok, more_data} ->
            process_master_command(data <> more_data, socket)
          {:error, reason} ->
            Logger.error("Error receiving additional data: #{inspect(reason)}")
        end
    end
  end

  # ------------------------------------------------------------------------
  # Replicas process

  defp perform_handshake(socket, replica_port) do
    {:ok, state} = :inet.getstat(socket)
    IO.puts("Socket state before handshake: #{inspect(state)}")
    with :ok <- send_ping(socket),
         :ok <- send_replconf_listening_port(socket, replica_port),
         :ok <- send_replconf_capa(socket),
         :ok <- send_psync(socket) do
        #  :ok <- receive_rdb_file(socket) do
        #  :ok <- start_listening_for_master_commands(socket) do
          {:ok, state} = :inet.getstat(socket)
          IO.puts("Socket state after handshake: #{inspect(state)}")
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
    packed_command = Server.Protocol.pack(["PSYNC", "?", "-1"]) |> IO.iodata_to_binary()
    case :gen_tcp.send(socket, packed_command) do
      :ok ->
        receive_psync_response(socket)
      {:error, reason} ->
        IO.puts("Failed to send PSYNC command")
        {:error, reason}
    end
  end

  defp receive_psync_response(socket) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, "+FULLRESYNC " <> rest} ->
        [_repl_id, _offset] = String.split(String.trim(rest), " ")
        # Logger.info("PSYNC successful. Replication ID: #{repl_id}, Offset: #{offset}")
        # receive_rdb_file(socket)
        # start_listening_for_master_commands(socket)
        :ok
      {:ok, response} ->
        IO.puts("Unexpected PSYNC response: #{inspect(response)}")
        {:error, :unexpected_response}
      {:error, reason} ->
        IO.puts("Error receiving PSYNC response: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # defp receive_rdb_file(socket) do
  #   case :gen_tcp.recv(socket, 0, 5000) do
  #     {:ok, data} ->
  #       IO.puts("Received data: #{inspect(String.slice(data, 0, 50))}...")  # Log the first 50 chars
  #       case Server.Protocol.parse(data) do
  #         {:ok, parsed_data, _rest} ->
  #           IO.puts("Parsed data: #{inspect(parsed_data)}")  # Debug line
  #         end
  #       case data do
  #         "$" <> rest ->
  #           [length_str, file_data] = String.split(rest, "\r\n", parts: 2)
  #           length = String.to_integer(length_str)
  #           if byte_size(file_data) < length do
  #             {:ok, remaining_data} = :gen_tcp.recv(socket, length - byte_size(file_data), 5000)
  #             _file_data = file_data <> remaining_data
  #           end
  #           IO.puts("Received RDB file of size #{byte_size(file_data)} bytes")
  #           :ok
  #         _ ->
  #           IO.puts("Invalid RDB format: doesn't start with $")
  #           {:error, :invalid_rdb_format}
  #       end
  #     {:error, reason} ->
  #       IO.puts("Error receiving RDB file: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end

  defp receive_rdb_file(socket) do
    Logger.info("Starting to receive data from socket")
    receive_data(socket, "", 0)
  end

  defp receive_data(socket, buffer, expected_length) do
    Logger.debug("Receiving data. Buffer size: #{byte_size(buffer)}, Expected length: #{expected_length}")
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} ->
        Logger.debug("Received chunk of size: #{byte_size(data)} bytes")
        new_buffer = buffer <> data
        Logger.debug("New buffer size: #{byte_size(new_buffer)} bytes")
        case parse_data(new_buffer, expected_length) do
          {:continue, remaining, new_expected_length} ->
            Logger.info("Incomplete data, continuing to receive. New expected length: #{new_expected_length}")
            receive_data(socket, remaining, new_expected_length)
          {:rdb_complete, rdb_data} ->
            Logger.info("Received complete RDB file of size #{byte_size(rdb_data)} bytes")
            {:ok, :rdb_complete, rdb_data}
          {:commands, commands} ->
            Logger.info("Received complete command: #{inspect(commands)}")
            # case command do
            #   [_command | args] ->
            #     execute_set_command(args)
            # end
            {:ok, :commands, commands}
        end
      {:error, reason} ->
        Logger.error("Error receiving data: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp parse_data("$" <> rest, 0) do
    Logger.debug("Parsing data starting with '$'")
    case String.split(rest, "\r\n", parts: 2) do
      [length_str, remaining] ->
        length = String.to_integer(length_str)
        Logger.info("Found RDB length indicator: #{length} bytes")
        parse_data(remaining, length)
      _ ->
        Logger.debug("Incomplete length indicator, continuing to receive")
        {:continue, "$" <> rest, 0}
    end
  end

  defp parse_data(data, expected_length) when byte_size(data) >= expected_length do
    Logger.debug("Received enough data to potentially complete RDB or command")
    <<rdb_data::binary-size(expected_length), rest::binary>> = data
    case rest do
      "" ->
        Logger.info("Completed receiving RDB file")
        {:rdb_complete, rdb_data}
      _ ->
        Logger.info("RDB file complete, parsing remaining data as command")
        parse_command(rest)
    end
  end

  defp parse_data(data, expected_length) do
    Logger.debug("Incomplete data, need #{expected_length - byte_size(data)} more bytes")
    {:continue, data, expected_length}
  end

  # defp parse_command(data) do
  #   IO.puts("Received data: #{inspect(String.slice(data, 100))}...")
  #   Logger.debug("Attempting to parse command from data")
  #   case Server.Protocol.parse(data) do
  #     {:ok, parsed_command, _rest} ->
  #       Logger.info("Successfully parsed command: #{inspect(parsed_command)}")
  #       # case parsed_command do
  #       #   [_command | args] ->
  #       #     execute_set_command(args)
  #       # end
  #       {:command, parsed_command}
  #     {:continuation, _} ->
  #       Logger.debug("Incomplete command, continuing to receive")
  #       {:continue, data, 0}
  #   end

  defp parse_command(data) do
    Logger.info("Received data: #{inspect(String.slice(data, 0, 90))}...")
    Logger.debug("Attempting to parse commands from data")
    case parse_multiple_commands(data, []) do
      {:ok, commands} ->
        Logger.info("Successfully parsed commands: #{inspect(commands)}")
        {:commands, commands}
      {:continue, remaining_data} ->
        Logger.debug("Incomplete command(s), continuing to receive")
        {:continue, remaining_data, 0}
    end
  end

  defp parse_multiple_commands(data, acc) do
    Logger.debug("Parsing data: #{inspect(data)}")
    case Server.Protocol.parse(data) do
      {:ok, parsed_command, rest} ->
        Logger.debug("Parsed command: #{inspect(parsed_command)}, remaining: #{inspect(rest)}")
        case parsed_command do
          [_command | _args] = command ->
            new_acc = acc ++ [command]
            if rest == "" do
              Logger.debug("Finished parsing all commands: #{inspect(new_acc)}")
              {:ok, new_acc}
            else
              parse_multiple_commands(rest, new_acc)
            end
          _ ->
            Logger.warning("Unexpected command format: #{inspect(parsed_command)}")
            {:ok, acc}
        end
      {:continuation, _} ->
        if acc == [] do
          Logger.debug("Incomplete data, need more: #{inspect(data)}")
          {:continue, data}
        else
          Logger.debug("Partial parse complete, commands: #{inspect(acc)}")
          {:ok, acc}
        end
    end
  end



  defp execute_set_command([command | args]) do
    case String.upcase(command) do
      "SET" ->
        [key, value | rest] = args
        case rest do
          ["PX", time] ->
            time_ms = String.to_integer(time)
            Server.Store.update(key, value, time_ms)
          [] ->
            Server.Store.update(key, value)
        end
      _ ->
        Logger.warning("Unhandled command from master: #{command}")
    end
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



  #----------------------------------------------------------------------------------

  # Server Code
  defp loop_acceptor(socket, config) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        spawn(fn -> serve(client, config) end)
        loop_acceptor(socket, config)
      {:error, reason} ->
        {:error, reason}
    end
  end

  # defp is_master_connection?(client, config) do
  #   case :inet.peername(client) do
  #     {:ok, {address, port}} ->
  #       is_master =
  #         case config.replica_of do
  #           {_master_host, master_port} ->
  #             port == master_port
  #           _ ->
  #             false
  #         end

  #       IO.puts("Connection check: #{inspect(address)}:#{port} - Is master? #{is_master}")
  #       is_master

  #     _ ->
  #       IO.puts("Failed to get peer name")
  #       false
  #   end
  # end

  defp serve(client, config) do
    try do
      client
      |> read_line()
      |> process_command(client, config)

      serve(client, config)
    catch
      kind, reason ->
        {:error, {kind, reason, __STACKTRACE__}}
    end
  end


  defp read_line(client) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} -> data
      {:error, reason} -> {:error, reason}
    end
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
    IO.puts("Expected Propagated Commands: #{inspect(commands)}")
    clients = Server.Clientbuffer.get_clients()

    Enum.each(clients, fn client ->
      Enum.each(commands, fn command ->
        packed_command = Server.Protocol.pack(command) |> IO.iodata_to_binary()
        case :gen_tcp.send(client, packed_command) do
          :ok -> :ok
          {:error, reason} ->
            IO.puts("Failed to send command to replica: #{inspect(reason)}")
        end
      end)
    end)
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
      Server.Clientbuffer.add_client(client)
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
        [command, time] ->
          command = String.upcase(to_string(command))
          if command == "PX" do
            time_ms = String.to_integer(time)
            Server.Store.update(key, value, time_ms)
          end
        [] ->
          Server.Store.update(key, value)
      end

      write_line("+OK\r\n", client)
      Server.Commandbuffer.add_command(["SET", key, value | rest])

      send_buffered_commands_to_replica()
      :ok
    catch
      _ ->
        write_line("-ERR Internal server error\r\n", client)
    end
  end

  defp execute_command("GET", [key], client) do
    IO.puts("Executing GET command for key: #{key}")
    case Server.Store.get_value_or_false(key) do
      {:ok, value} ->
        IO.puts("Value found: #{value}")
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
