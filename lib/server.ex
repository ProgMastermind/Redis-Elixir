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
      Server.Bytes,
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
    {:ok, socket} = :gen_tcp.listen(config.port, [:binary, active: false, reuseaddr: true, buffer: 1024*1024])

    if config.replica_of do
      spawn(fn ->
         connect_to_master(config.replica_of, config.port)
      end)
    end

    loop_acceptor(socket, config)
  end

  defp connect_to_master({master_host, master_port}, replica_port) do
    case :gen_tcp.connect(to_charlist(master_host), master_port, [:binary, active: false, buffer: 8192]) do
      {:ok, socket} ->
        {:ok, {remote_address, remote_port}} = :inet.peername(socket)
        {:ok, {local_address, local_port}} = :inet.sockname(socket)
        Logger.info("Connected to #{:inet.ntoa(remote_address)}:#{remote_port} from #{:inet.ntoa(local_address)}:#{local_port}")
        case perform_handshake(socket, replica_port) do
          :ok ->
            Logger.info("Handshake completed successfully")
            # handle_rdb_and_commands(socket)
            # handle_commands(socket)
            # {:ok, socket}
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
    with :ok <- send_ping(socket),
         :ok <- send_replconf_listening_port(socket, replica_port),
         :ok <- send_replconf_capa(socket),
         :ok <- send_psync(socket) do
      :ok
    else
      {:error, reason} ->
        IO.puts("Handshake failed: #{inspect(reason)}")
        # :gen_tcp.close(socket)
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


  #second one
  # defp receive_psync_response(socket) do
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, data} ->
  #       Logger.debug("Received PSYNC response: #{inspect(data)}")
  #       case parse_psync_response(data) do
  #         {:ok, repl_id, offset, remaining_data} ->
  #           Logger.info("PSYNC successful. Replication ID: #{repl_id}, Offset: #{offset}, Remaining Data: #{remaining_data}")
  #           handle_rdb_and_commands(socket)
  #           :ok
  #         {:error, reason} ->
  #           Logger.warning("Error parsing PSYNC response: #{reason}")
  #           {:error, reason}
  #       end
  #     {:error, reason} ->
  #       Logger.error("Error receiving PSYNC response: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end

  # third one
  # defp receive_psync_response(socket) do
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, data, rest, bytes_read} ->
  #       Logger.debug("Received PSYNC response: #{inspect(data)}, bytes read: #{bytes_read}")

  #       case parse_psync_response(data) do
  #         {:ok, repl_id, offset, _remaining_data} ->
  #           Logger.info("PSYNC successful. Replication ID: #{repl_id}, Offset: #{offset}")
  #           handle_rdb_and_commands(socket, rest)
  #           :ok
  #         {:error, reason} ->
  #           Logger.warning("Error parsing PSYNC response: #{reason}")
  #           {:error, reason}
  #       end
  #     {:error, reason} ->
  #       Logger.error("Error receiving PSYNC response: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end

  #fouth one
  defp receive_psync_response(socket) do
    case :gen_tcp.recv(socket, 56, 5000) do
      {:ok, data} ->
        Logger.debug("Received PSYNC response: #{inspect(data)}")
        case parse_psync_response(data) do
          {:ok, repl_id, offset, _remaining_data} ->
            Logger.info("PSYNC successful. Replication ID: #{repl_id}, Offset: #{offset}")
            handle_rdb_and_commands(socket)
          {:error, reason} ->
            Logger.warning("Error parsing PSYNC response: #{reason}")
            {:error, reason}
        end
      {:error, reason} ->
        Logger.error("Error receiving PSYNC response: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp handle_rdb_and_commands(socket) do
    case :gen_tcp.recv(socket, 93, 5000) do
      {:ok, rdb_data} ->
        Logger.info("RDB data received, length: #{byte_size(rdb_data)}")
        # Process RDB data here if needed
        parse_commands(socket)
      {:error, reason} ->
        Logger.error("Error reading RDB: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def parse_commands(socket) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} ->
        Logger.debug("Received data chunk: #{inspect(data)}, bytes: #{byte_size(data)}")
        Server.Bytes.increment_offset(byte_size(data))
        case parse_command(data) do
          {:commands, commands} ->
            Enum.each(commands, fn command ->
              execute_replica_command(socket, command)
            end)
        end
        parse_commands(socket)
      {:error, closed} ->
        {:error, closed}
    end
  end

  defp parse_command(data) do
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
    # Server.Bytes.increment_offset(byte_size(data))
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




  # # #-------------------------------------------------------------------
  # defp handle_commands(socket) do
  #   case read_command(socket) do
  #     {:ok, command} ->
  #       Logger.info("Received command: #{inspect(command)}")
  #       # Server.Bytes.increment_offset(bytes_read)
  #       execute_replica_command(socket, command)
  #       handle_commands(socket)
  #     {:error, :timeout}->
  #       Logger.warning("Timeout while reading command")
  #       handle_commands(socket)
  #     {:error, :closed} ->
  #       Logger.info("Connection closed")
  #       :ok
  #     {:error, reason} ->
  #       Logger.error("Error reading command: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end


  # defp read_command(socket) do
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, "*" <> num_args_str} ->
  #       num_args = String.to_integer(num_args_str)
  #       read_args(socket, num_args, [])
  #     {:error, reason} ->
  #       {:error, reason}
  #   end
  # end

  # defp read_args(_socket, 0, acc), do: {:ok, Enum.reverse(acc)}
  # defp read_args(socket, num_args, acc) do
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, "$" <> len_str} ->
  #       len = String.to_integer(len_str)
  #       case read_exact(socket, len) do
  #         {:ok, arg} ->
  #           case read_until_delimiter(socket, "\r\n") do
  #             {:ok, ""} ->
  #               read_args(socket, num_args - 1, [arg | acc])
  #             {:error, reason} ->
  #               {:error, reason}
  #           end
  #         {:error, reason} ->
  #           {:error, reason}
  #       end
  #     {:error, reason} ->
  #       {:error, reason}
  #   end
  # end

  # defp read_until_delimiter(socket, delimiter) do
  #   read_until_delimiter(socket, delimiter, "")
  # end

  # defp read_until_delimiter(socket, delimiter, acc) do
  #   case :gen_tcp.recv(socket, 0, 5000) do
  #     {:ok, data} ->
  #       new_acc = acc <> data
  #       case String.split(new_acc, delimiter, parts: 2) do
  #         [result, rest] ->
  #           :gen_tcp.unrecv(socket, rest)
  #           {:ok, result}
  #         [_] ->
  #           read_until_delimiter(socket, delimiter, new_acc)
  #       end
  #     {:error, :timeout} ->
  #       {:error, :timeout}
  #     {:error, reason} ->
  #       {:error, reason}
  #   end
  # end

  # defp read_exact(socket, length) do
  #   :gen_tcp.recv(socket, length, 5000)
  # end
  #--------------------------------------------------------------------------
   # checking without unrecv operation

  #  defp handle_rdb_and_commands(socket) do
  #   case read_rdb(socket) do
  #     {:ok, rdb_data} ->
  #       Logger.info("Received complete RDB file of size #{byte_size(rdb_data)} bytes")
  #       # Process RDB data here if needed
  #       # handle_commands(socket)
  #       :ok
  #       {:error, :closed} ->
  #         Logger.debug("Connection closed")
  #         :ok
  #       {:error, reason} ->
  #         Logger.error("Error reading command: #{inspect(reason)}")
  #   end
  # end

  #  defp read_rdb(socket) do
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, "$" <> size_str, _bytes_read} ->
  #       size = String.to_integer(size_str)
  #       read_exact(socket, size)
  #     {:error, reason} ->
  #       {:error, reason}
  #   end
  # end

  # defp handle_commands(socket) do
  #   case read_command(socket) do
  #     {:ok, command, command_bytes} ->
  #       Logger.info("Received command: #{inspect(command)}, total bytes: #{command_bytes}")
  #       execute_replica_command(socket, command, command_bytes)
  #       handle_commands(socket)
  #     {:error, :closed} ->
  #       Logger.info("Connection closed")
  #       :ok
  #     {:error, :timeout} ->
  #       handle_commands(socket)
  #     {:error, reason} ->
  #       Logger.error("Error reading command: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end

  # defp read_command(socket) do
  #   Logger.debug("Starting to read command")
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, "*" <> num_args_str, first_line_bytes} ->
  #       num_args = String.to_integer(num_args_str)
  #       # Logger.debug("Command header: *#{num_args}, bytes: #{first_line_bytes}")
  #       read_command_args(socket, num_args, [], first_line_bytes)
  #     {:error, :timeout} ->
  #       {:error, :timeout}
  #       {:error, reason} ->
  #         Logger.error("Error reading command header: #{inspect(reason)}")
  #         {:error, reason}
  #   end
  # end

  # defp read_command_args(_socket, 0, acc, total_bytes) do
  #   Logger.debug("Finished reading all arguments. Total bytes: #{total_bytes}")
  #   {:ok, Enum.reverse(acc), total_bytes}
  # end
  # defp read_command_args(socket, num_args, acc, total_bytes) do
  #   # Logger.debug("Reading argument #{length(acc) + 1} of #{num_args + length(acc)}")
  #   case read_until_delimiter(socket, "\r\n") do
  #     {:ok, "$" <> len_str, bytes_read} ->
  #       len = String.to_integer(len_str)
  #       # Logger.debug("Argument length indicator: $#{len}, bytes: #{bytes_read}")
  #       case read_exact(socket, len) do
  #         {:ok, arg} ->
  #           # Logger.debug("Read argument: #{inspect(arg)}, bytes: #{len}")
  #           case read_until_delimiter(socket, "\r\n") do
  #             {:ok, "", more_bytes} ->
  #               new_total = total_bytes + bytes_read + len + more_bytes
  #               Logger.debug("Argument total bytes: #{new_total - total_bytes}")
  #               read_command_args(socket, num_args - 1, [arg | acc], new_total)
  #             {:error, reason} ->
  #               Logger.error("Error reading argument delimiter: #{inspect(reason)}")
  #               {:error, reason}
  #           end
  #         {:error, reason} ->
  #           Logger.error("Error reading argument content: #{inspect(reason)}")
  #           {:error, reason}
  #       end
  #     {:error, reason} ->
  #       Logger.error("Error reading argument length: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end

  # defp read_until_delimiter(socket, delimiter) do
  #   # Logger.debug("Reading until delimiter: #{inspect(delimiter)}")
  #   read_until_delimiter(socket, delimiter, "", 0)
  # end

  # defp read_until_delimiter(socket, delimiter, acc, bytes_read) do
  #   case :gen_tcp.recv(socket, 0, 1000) do
  #     {:ok, data} ->
  #       Logger.debug("Received data chunk: #{inspect(data)}, bytes: #{byte_size(data)}")
  #       new_acc = acc <> data
  #       new_bytes_read = bytes_read + byte_size(data)
  #       case String.split(new_acc, delimiter, parts: 2) do
  #         [result, rest] ->
  #           # Logger.debug("Found delimiter. Result: #{inspect(result)}, remaining: #{byte_size(rest)} bytes")
  #           :gen_tcp.unrecv(socket, rest)
  #           {:ok, result, new_bytes_read - byte_size(rest)}
  #         [_] ->
  #           Logger.debug("Delimiter not found, continuing to read")
  #           read_until_delimiter(socket, delimiter, new_acc, new_bytes_read)
  #       end
  #     {:error, reason} ->
  #       Logger.error("Error receiving data: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end

  # defp read_exact(socket, length) do
  #   Logger.debug("Reading exact #{length} bytes")
  #   case :gen_tcp.recv(socket, length) do
  #     {:ok, data} ->
  #       Logger.debug("Read exact data: #{inspect(data)}")
  #       {:ok, data}
  #     {:error, reason} ->
  #       Logger.error("Error reading exact bytes: #{inspect(reason)}")
  #       {:error, reason}
  #   end
  # end
  #--------------------------------------------------------------------------

  #--------------------------------------------------------------------
  defp read_until_delimiter(socket, delimiter, acc \\ "", bytes_read \\ 0) do
    case :gen_tcp.recv(socket, 0, 5000) do  # Add a timeout to prevent infinite waiting
      {:ok, data} ->
        Logger.debug("Received data chunk: #{inspect(data)}, bytes: #{byte_size(data)}")
        new_acc = acc <> data
        new_bytes_read = bytes_read + byte_size(data)
        case String.split(new_acc, delimiter, parts: 2) do
          [result, rest] ->
            Logger.debug("Found delimiter. Result: #{inspect(result)}, remaining: #{byte_size(rest)} bytes")
            {:ok, result, rest, new_bytes_read - byte_size(rest)}
          [_] ->
            Logger.debug("Delimiter not found, continuing to read")
            read_until_delimiter(socket, delimiter, new_acc, new_bytes_read)
        end
      {:error, reason} ->
        Logger.error("Error receiving data: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_command(socket, rest) do
    Logger.debug("Starting to read command")
    case read_until_delimiter(socket, "\r\n", rest) do
      {:ok, "*" <> num_args_str, new_rest, first_line_bytes} ->
        num_args = String.to_integer(num_args_str)
        read_command_args(socket, num_args, [], first_line_bytes, new_rest)
      {:error, :timeout} ->
        {:error, :timeout}
      {:error, reason} ->
        Logger.error("Error reading command header: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_command_args(_socket, 0, acc, total_bytes, _rest) do
    Logger.debug("Finished reading all arguments. Total bytes: #{total_bytes}")
    {:ok, Enum.reverse(acc), total_bytes}
  end

  defp read_command_args(socket, num_args, acc, total_bytes, rest) do
    case read_until_delimiter(socket, "\r\n", rest) do
      {:ok, "$" <> len_str, new_rest, bytes_read} ->
        len = String.to_integer(len_str)
        case read_exact(socket, len, new_rest) do
          {:ok, arg, remaining} ->
            case read_until_delimiter(socket, "\r\n", remaining) do
              {:ok, "", final_rest, more_bytes} ->
                new_total = total_bytes + bytes_read + len + more_bytes
                Logger.debug("Argument total bytes: #{new_total - total_bytes}")
                read_command_args(socket, num_args - 1, [arg | acc], new_total, final_rest)
              {:error, reason} ->
                Logger.error("Error reading argument delimiter: #{inspect(reason)}")
                {:error, reason}
            end
          {:error, reason} ->
            Logger.error("Error reading argument content: #{inspect(reason)}")
            {:error, reason}
        end
      {:error, reason} ->
        Logger.error("Error reading argument length: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_exact(socket, length, buffer) do
    if byte_size(buffer) >= length do
      <<value::binary-size(length), rest::binary>> = buffer
      {:ok, value, rest}
    else
      case :gen_tcp.recv(socket, length - byte_size(buffer), 5000) do
        {:ok, data} ->
          new_buffer = buffer <> data
          <<value::binary-size(length), rest::binary>> = new_buffer
          {:ok, value, rest}
        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  #------------------------------------------------------------------------

  #---------------------------------------------------------
  # ACK Commands

  defp execute_replica_command(socket, command) do
    case command do
      ["SET" | args] ->
        execute_set_command(["SET" | args])
      ["REPLCONF", "GETACK", "*"] ->
        send_replconf_ack(socket)
      ["PING"] ->
        Logger.info("PING has executed")
      _ ->
        Logger.warning("Unhandled command from master : #{inspect(command)}")
    end
  end

  # defp execute_replica_command(_socket, ["SET" | args], command_bytes) do
  #   Server.Bytes.increment_offset(command_bytes)
  #   execute_set_command(["SET" | args])
  # end

  # defp execute_replica_command(socket, ["REPLCONF", "GETACK", "*"], command_bytes) do
  #   send_replconf_ack(socket, command_bytes)
  # end

  # defp execute_replica_command(_socket, ["PING"], command_bytes) do
  #   Server.Bytes.increment_offset(command_bytes)
  #   # Logger.info("Processed command: #{inspect(command)}, incremented offset by #{command_bytes}")
  # end

  # defp execute_replica_command(_socket, command, _command_bytes) do
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
    offset = Server.Bytes.get_offset()
    Logger.info("Executing REPLCONF GETACK. Current offset: #{offset}")
    command = ["REPLCONF", "ACK", "#{offset}"]
    packed_command = Server.Protocol.pack(command) |> IO.iodata_to_binary()
    case :gen_tcp.send(socket, packed_command) do
      :ok ->
        Logger.info("Sent REPLCONF ACK response: #{inspect(packed_command)}")
      {:error, reason} ->
        Logger.error("Failed to send REPLCONF ACK: #{inspect(reason)}")
    end
    # Server.Bytes.increment_offset(37)
    # Server.Bytes.increment_offset(command_bytes)
    # offer = Server.Bytes.get_offset()
    # Logger.info("Stored Bytes: #{offer}")
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

  # defp execute_command("REPLCONF", args, client) do
  #   IO.puts("Received REPLCONF with args: #{inspect(args)}")

  #   message = "+OK\r\n"
  #   write_line(message, client)
  # end

  defp execute_command("REPLCONF", args, client) do
    Logger.info("Received REPLCONF with args: #{inspect(args)}")

    case args do
      ["listening-port", port] ->
        Logger.info("Replica reported listening on port: #{port}")
        message = "+OK\r\n"
        write_line(message, client)

      ["capa", capability] ->
        Logger.info("Replica reported capability: #{capability}")
        message = "+OK\r\n"
        write_line(message, client)

      ["ACK", offset] ->
        # Handle ACK from replica
        Logger.info("Received ACK from replica with offset: #{offset}")
        # You might want to update some internal state here
        :ok

      _ ->
        Logger.warning("Received unknown REPLCONF subcommand: #{inspect(args)}")
        message = "-ERR Unknown REPLCONF subcommand\r\n"
        write_line(message, client)
    end
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

  # defp execute_command("REPLCONF", args, _client) do
  #   Logger.info("Received REPLCONF with args: #{inspect(args)}")
  #   case args do
  #     ["ACK", offset] ->
  #       # Handle ACK from replica
  #       Logger.info("Received ACK from replica with offset: #{offset}")
  #       :ok
  #     _ ->
  #       # Handle other REPLCONF subcommands if needed
  #       :ok
  #   end
  # end

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
