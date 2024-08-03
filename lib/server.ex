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
      spawn(fn ->
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
          {:error, reason} ->
            Logger.error("Handshake failed: #{inspect(reason)}")
        end
      {:error, reason} ->
        Logger.error("Failed to connect to master: #{inspect(reason)}")
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

  #--------------------------------------------------------
  # main one
  # defp receive_psync_response(socket) do
  #   #---------------------------------------------------------------
  #   #now this is for checking data which comes with header
  #   case :gen_tcp.recv(socket, 0, :infinity) do
  #     {:ok, data} ->
  #       # Logger.debug("Received PSYNC response: #{inspect(data, limit: :infinity, binaries: :as_binaries)}")
  #       # Logger.debug("Received PSYNC response: #{Base.encode16(data)}")
  #       Logger.debug("Received PSYNC response: #{inspect(data)}")
  #       case parse_psync_response(data) do
  #         {:ok, repl_id, offset, remaining_data} ->
  #           Logger.info("PSYNC successful. Replication ID: #{repl_id}, Offset: #{offset}, Remaining Data: #{remaining_data}")
  #           handle_rdb_and_commands(socket)
  #           # if remaining_data != "" do
  #           #   case parse_data(remaining_data, 0) do
  #           #     {:rdb_complete, rdb_data} ->
  #           #       Logger.info("Received complete RDB file of size #{byte_size(rdb_data)} bytes")
  #           #       {:commands, commands} ->
  #           #         Enum.each(commands, fn command ->
  #           #           Logger.info("Executing command: #{inspect(command)}")
  #           #           execute_replica_command(socket, command)
  #           #         end)
  #           #       {:error, reason} ->
  #           #         Logger.error("Failed to receive RDB file: #{inspect(reason)}")
  #           #       end
  #           # end

  #           # case receive_rdb_file(socket) do
  #           #   {:ok, :rdb_complete, rdb_data} ->
  #           #     process_rdb_data(rdb_data)
  #           #   {:ok, :commands, commands} ->
  #           #     Enum.each(commands, fn command ->
  #           #       Logger.info("Executing command: #{inspect(command)}")
  #           #       execute_set_command(command)
  #           #     end)
  #           #   {:error, reason} ->
  #           #     Logger.error("Failed to receive RDB file: #{inspect(reason)}")
  #           # end

  #           # :ok
  #         {:error, reason} ->
  #           Logger.warning("Error parsing PSYNC response: #{reason}")
  #           {:error, reason}
  #       end

  #     {:error, reason} ->
  #       Logger.error("Error receiving PSYNC response: #{inspect(reason)}")
  #       {:error, reason}
  #   end

  # end

  #second one
  defp receive_psync_response(socket) do
    case read_until_delimiter(socket, "\r\n") do
      {:ok, data} ->
        Logger.debug("Received PSYNC response: #{inspect(data)}")
        case parse_psync_response(data) do
          {:ok, repl_id, offset, remaining_data} ->
            Logger.info("PSYNC successful. Replication ID: #{repl_id}, Offset: #{offset}, Remaining Data: #{remaining_data}")
            handle_rdb_and_commands(socket)
            :ok
          {:error, reason} ->
            Logger.warning("Error parsing PSYNC response: #{reason}")
            {:error, reason}
        end
      {:error, reason} ->
        Logger.error("Error receiving PSYNC response: #{inspect(reason)}")
        {:error, reason}
    end
  end

  #-------------------------------------------------------------



  defp parse_psync_response(data) do
    parts = String.split(data, "\r\n", parts: 2)

    case parts do
      [psync_part, rdb_part] ->
        parse_psync_part(psync_part, rdb_part)
      [psync_part] ->
        parse_psync_part(psync_part, "")
      _ ->
        {:error, :invalid_psync_response}
    end
  end

  defp parse_psync_part(psync_part, rdb_part) do
    case Regex.run(~r/^\+FULLRESYNC (\S+) (\d+)$/, psync_part, capture: :all_but_first) do
      [repl_id, offset_str] ->
        offset = String.to_integer(offset_str)
        {:ok, repl_id, offset, rdb_part}
      nil ->
        {:error, :invalid_psync_response}
    end
  end


  # ---------------------------------------------------------------
  # Main working receive_rdb
  defp receive_rdb_file(socket) do
    Logger.info("Starting to receive data from socket")
    receive_data(socket, "", 0)
  end

  defp receive_data(socket, buffer, expected_length) do
    Logger.debug("Receiving data. Buffer size: #{byte_size(buffer)}, Expected length: #{expected_length}")
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} ->
        Logger.debug("Received raw bytes: #{inspect(data, limit: :infinity, binaries: :as_binaries)}")
        Logger.debug("Received chunk of size: #{byte_size(data)} bytes")
        Logger.debug("Received data (as string): #{inspect(data, charlists: :as_lists)}")
        Logger.info("Received data: #{inspect(String.slice(data, 0, 100))}...")
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
    Logger.debug("Data size: #{byte_size(data)}, Expected length: #{expected_length}")
    <<rdb_data::binary-size(expected_length), rest::binary>> = data
    Logger.debug("RDB data size: #{byte_size(rdb_data)}, Rest size: #{byte_size(rest)}")
    case rest do
      <<>> ->
        Logger.info("Completed receiving the RDB file")
        {:rdb_complete, rdb_data}
      _ ->
        Logger.info("RDB file complete, parsing remaining data as command")
        Logger.debug("Remaining data: #{inspect(rest)}")
        parse_command(rest)
    end
  end

  defp parse_data(data, expected_length) do
    Logger.debug("Incomplete data, need #{expected_length - byte_size(data)} more bytes")
    {:continue, data, expected_length}
  end


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

  #----------------------------------------------------------------------------------

  #------------------------------------------------------------------------
  #reading upto certain bytes
  defp handle_rdb_and_commands(socket) do
    case read_rdb(socket) do
      {:ok, rdb_data} ->
        Logger.info("Received complete RDB file of size #{byte_size(rdb_data)} bytes")
        # Process RDB data here if needed
        handle_commands(socket)
      {:error, reason} ->
        Logger.error("Failed to receive RDB file: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_rdb(socket) do
    case read_until_delimiter(socket, "\r\n") do
      {:ok, "$" <> size_str} ->
        size = String.to_integer(size_str)
        read_exact(socket, size)
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_commands(socket) do
    case read_command(socket) do
      {:ok, command} ->
        Logger.info("Received command: #{inspect(command)}")
        # Server.Bytes.increment_offset(bytes_read)
        execute_replica_command(socket, command)
        handle_commands(socket)
      {:error, :closed} ->
        Logger.info("Connection closed")
        :ok
      {:error, reason} ->
        Logger.error("Error reading command: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_command(socket) do
    case read_until_delimiter(socket, "\r\n") do
      {:ok, "*" <> num_args_str} ->
        num_args = String.to_integer(num_args_str)
        read_args(socket, num_args, [])
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_args(_socket, 0, acc), do: {:ok, Enum.reverse(acc)}
  defp read_args(socket, num_args, acc) do
    case read_until_delimiter(socket, "\r\n") do
      {:ok, "$" <> len_str} ->
        len = String.to_integer(len_str)
        case read_exact(socket, len) do
          {:ok, arg} ->
            case read_until_delimiter(socket, "\r\n") do
              {:ok, ""} ->
                read_args(socket, num_args - 1, [arg | acc])
              {:error, reason} ->
                {:error, reason}
            end
          {:error, reason} ->
            {:error, reason}
        end
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_until_delimiter(socket, delimiter) do
    read_until_delimiter(socket, delimiter, "")
  end

  defp read_until_delimiter(socket, delimiter, acc) do
    case :gen_tcp.recv(socket, 0, :infinity) do
      {:ok, data} ->
        new_acc = acc <> data
        case String.split(new_acc, delimiter, parts: 2) do
          [result, rest] ->
            :gen_tcp.unrecv(socket, rest)
            {:ok, result}
          [_] ->
            read_until_delimiter(socket, delimiter, new_acc)
        end
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_exact(socket, length) do
    :gen_tcp.recv(socket, length)
  end

  #--------------------------------------------------------------------------


  #---------------------------------------------------------
  # ACK Commands

  defp execute_replica_command(socket, command) do
    case command do
      ["SET" | args] ->
        execute_set_command(["SET" | args])
      ["REPLCONF", "GETACK", "*"] ->
        send_replconf_ack(socket)
      _ ->
        Logger.warning("Unhandled command from master : #{inspect(command)}")
    end
  end

  # defp execute_replica_command(socket, ["SET" | args]) do
  #   execute_set_command(["SET" | args])
  # end

  # defp execute_replica_command(socket, ["REPLCONF", "GETACK", "*"]) do
  #   send_replconf_ack(socket)
  # end

  # defp execute_replica_command(socket, command) do
  #   Logger.warning("Unhandled command from master: #{inspect(command)}")
  # end

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
        Logger.warning("Unhandled command from the master: #{command}")
    end
  end

  defp send_replconf_ack(socket) do
    ack_command = ["REPLCONF", "ACK", "0"]
    packed_command = Server.Protocol.pack(ack_command) |> IO.iodata_to_binary()
    case :gen_tcp.send(socket, packed_command) do
      :ok ->
        Logger.info("Sent REPLCONF ACK 0 to master")
      {:error, reason} ->
        Logger.error("Failed to send REPLCONF ACK: #{inspect(reason)}")
    end
  end

  #-----------------------------------------------------------------


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
      {:ok, received_data} ->
        Logger.debug("Received raw bytes: #{inspect(received_data, limit: :infinity, binaries: :as_binaries)}")
        if received_data == expected_response do
          Logger.debug("Received expected response")
          :ok
        else
          Logger.warning("Unexpected response. Expected: #{inspect(expected_response)}, Received: #{inspect(received_data)}")
          {:error, :unexpected_response}
        end
      {:error, reason} ->
        Logger.error("Error receiving response: #{inspect(reason)}")
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
    # IO.puts("Expected Propagated Commands: #{inspect(commands)}")
    clients = Server.Clientbuffer.get_clients()

    Logger.debug("Sending buffered commands to replicas: #{inspect(commands)}")
    Logger.debug("Number of clients: #{length(clients)}")

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

  defp execute_command("REPLCONF", args, _client) do
    Logger.info("Received REPLCONF with args: #{inspect(args)}")
    case args do
      ["ACK", offset] ->
        # Handle ACK from replica
        Logger.info("Received ACK from replica with offset: #{offset}")
        :ok
      _ ->
        # Handle other REPLCONF subcommands if needed
        :ok
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
