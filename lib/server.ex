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
      Server.Acknowledge,
      Server.Pendingwrites,
      Server.Config,
      Server.RdbStore,
      {Task, fn -> Server.listen(config) end}
    ]

    opts = [strategy: :one_for_one, name: :sup]
    {:ok, pid} = Supervisor.start_link(children, opts)

    set_initial_config(config)
    load_rdb()

    {:ok, pid}
  end

  defp parse_args do
    {opts, _, _} = OptionParser.parse(System.argv(),
      switches: [port: :integer, replicaof: :string, dir: :string, dbfilename: :string])

    port = opts[:port] || 6379
    replica_of = parse_replicaof(opts[:replicaof])
    dir = opts[:dir]
    dbfilename = opts[:dbfilename]

    %{port: port, replica_of: replica_of, dir: dir, dbfilename: dbfilename}
  end

  defp parse_replicaof(nil), do: nil
  defp parse_replicaof(replicaof) do
    [host, port] = String.split(replicaof, " ")
    {host, String.to_integer(port)}
  end

  defp set_initial_config(config) do
    Logger.info("Configuring dir: #{config.dir}")
    Logger.info("Configuring dbname: #{config.dbfilename}")
    if config.dir, do: Server.Config.set_config("dir", config.dir)
    if config.dbfilename, do: Server.Config.set_config("dbfilename", config.dbfilename)
  end

  defp load_rdb do
    rdb_path = Server.Config.get_rdb_path()
    Logger.info("RDB file path is: #{rdb_path}")
    Server.RdbStore.load_rdb(rdb_path)
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
            parse_commands(socket)
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
        :ok
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
        Server.Acknowledge.increment_ack_count();
        Logger.info("Received ACK from replica with offset: #{offset}")
        count = Server.Acknowledge.get_ack_count();
        Logger.info("Received acknowledgement from #{count} replicas")
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

      Server.Pendingwrites.set_pending_writes()
      write_line("+OK\r\n", client)

      Server.Commandbuffer.add_command(["SET", key, value | rest])
      send_buffered_commands_to_replica()

      :ok
    catch
      _ ->
        write_line("-ERR Internal server error\r\n", client)
    end
  end

  defp execute_command("KEYS", ["*"], client) do
    Logger.info("Executing keys")
    keys = Server.RdbStore.get_keys()
    response = Server.Protocol.pack(keys) |> IO.iodata_to_binary()
    write_line(response, client)
  end


  defp execute_command("GET", [key], client) do
    IO.puts("Executing GET command for key: #{key}")

    rdb_state = Server.RdbStore.get_state()
    case Map.fetch(rdb_state, key) do
      {:ok, value} ->
        Logger.info("Value found in RDB store: #{inspect(value)}")
        response = Server.Protocol.pack(value) |> IO.iodata_to_binary()
        write_line(response, client)

      :error ->
        # If not found in RDB store, check the regular store
        case Server.Store.get_value_or_false(key) do
          {:ok, value} ->
            Logger.info("Value found in regular store: #{inspect(value)}")
            response = Server.Protocol.pack(value) |> IO.iodata_to_binary()
            write_line(response, client)

          {:error, _reason} ->
            write_line("$-1\r\n", client)
        end
    end
  end

  # defp execute_command("WAIT", _args, client) do
  #   Logger.info("sending reply to the client")
  #   replica_count = Server.Clientbuffer.get_client_count();
  #   write_line(":#{replica_count}\r\n", client)
  # end

  defp execute_command("WAIT", [_count, timeout], client) do
    Logger.info("Wait command is triggering")
    timeout = String.to_integer(timeout)

    if Server.Pendingwrites.pending_writes?() do
      Server.Acknowledge.reset_ack_count()

      Server.Clientbuffer.get_clients()
      |> Enum.each(fn replica_socket ->
        :gen_tcp.send(replica_socket, Server.Protocol.pack(["REPLCONF", "GETACK", "*"]))
      end)


      :ok = wait_and_respond(timeout, client)
      # wait_and_respond(count, timeout, start_time, client)
    else
      replica_count = Server.Clientbuffer.get_client_count();
      write_line(":#{replica_count}\r\n", client)
    end

  end

  defp wait_and_respond(timeout, client) do
    # Spawn a new process to handle the waiting and responding
    spawn(fn ->
      # Wait for the specified timeout
      Process.sleep(timeout)

      # After waiting, get the acknowledgment count and respond
      ack_count = Server.Acknowledge.get_ack_count()
      Logger.info("Acknowledge count: #{ack_count}")
      write_line(":#{ack_count}\r\n", client)
    end)

    # The main process continues immediately
    :ok
  end

  defp execute_command("CONFIG", ["GET", param], client) do
    value = Server.Config.get_config(param)
    Logger.info("Value for a dir: #{value}")
    response = if value do
      Server.Protocol.pack([param, value]) |> IO.iodata_to_binary()
    else
      "$-1\r\n"
    end
    write_line(response, client)
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
