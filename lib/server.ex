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
      Server.ClientState,
      Server.Streamstore,
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
  # Replica handling commands

  defp perform_handshake(socket, replica_port) do
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
  # handling of commands

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

  defp execute_command("ECHO", [message], client) do
    response = Server.Protocol.pack(message) |> IO.iodata_to_binary()
    write_line(response, client)
  end

  defp execute_command("SET", [key, value | rest], client) do
    if Server.ClientState.in_transaction?(client) do
      Server.ClientState.add_command(client, ["SET", key, value | rest])
      write_line("+QUEUED\r\n", client)
    else
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

  end

  defp execute_command("KEYS", ["*"], client) do
    Logger.info("Executing keys")
    keys = Server.RdbStore.get_keys()
    response = Server.Protocol.pack(keys) |> IO.iodata_to_binary()
    write_line(response, client)
  end


  defp execute_command("GET", [key] , client) do
    IO.puts("Executing GET command for key: #{key}")
    if Server.ClientState.in_transaction?(client) do
      Server.ClientState.add_command(client, ["GET", key])
      write_line("+QUEUED\r\n", client)
    else
      rdb_state = Server.RdbStore.get_state()
      case Map.fetch(rdb_state, key) do
        {:ok, value} ->
          # Logger.info("Value found in RDB store: #{inspect(value)}")
          case value do
            {value, expires_at} ->
              if DateTime.compare(expires_at, DateTime.utc_now()) == :gt do
                response = Server.Protocol.pack(value) |> IO.iodata_to_binary()
                write_line(response, client)
              else
                write_line("$-1\r\n", client)
              end
            _ ->
              response = Server.Protocol.pack(value) |> IO.iodata_to_binary()
              write_line(response, client)
          end

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

  end

  defp execute_command("INCR", [key], client) do
    if Server.ClientState.in_transaction?(client) do
      Server.ClientState.add_command(client, ["INCR", key])
      write_line("+QUEUED\r\n", client)
    else
      case Server.Store.get_value_or_false(key) do
        {:ok, value} ->
          case Integer.parse(value) do
            {int_value, _} ->
              increased_value = int_value + 1
              Server.Store.update(key, Integer.to_string(increased_value))
              write_line(":#{increased_value}\r\n", client)
            :error ->
              write_line("-ERR value is not an integer or out of range\r\n", client)
          end
        {:error, _reason} ->
          Server.Store.update(key, "1")
          write_line(":1\r\n", client)
      end
    end
  end

  defp execute_command("MULTI", _args, client) do
    if Server.ClientState.in_transaction?(client) do
      write_line("-ERR MULTI calls can not be nested\r\n", client)
    else
      Server.ClientState.start_transaction(client)
      write_line("+OK\r\n", client)
    end
  end

  defp execute_command("EXEC", _args, client) do
    Logger.info("EXEC is executing")
    if Server.ClientState.in_transaction?(client) do
      queued_commands = Server.ClientState.get_and_clear_commands(client)
      Logger.info("Queued commands: #{queued_commands}")
      Server.ClientState.end_transaction(client)

      results = Enum.map(queued_commands, fn command->
        Logger.info("Executing command: #{inspect(command)}")
        execute_queued_command(command, client)
      end)

      response = "*#{length(results)}\r\n" <> Enum.join(results)
      write_line(response, client)
    else
      write_line("-ERR EXEC without MULTI\r\n", client)
    end
  end

  defp execute_command("DISCARD", _args, client) do
    if Server.ClientState.in_transaction?(client) do
      Server.ClientState.end_transaction(client)
      write_line("+OK\r\n", client)
    else
      write_line("-ERR DISCARD without MULTI\r\n", client)
    end
  end

  defp execute_command("XADD", [stream_key, id | entry], client) do
    entry_map = Enum.chunk_every(entry, 2) |> Enum.into(%{}, fn [k, v] -> {k, v} end)

    case process_id(stream_key, id) do
      {:ok, final_id} ->
        result = Server.Streamstore.add_entry(stream_key, final_id, entry_map)
        response = Server.Protocol.pack(result) |> IO.iodata_to_binary()
        write_line(response, client)
      :ok ->
        result = Server.Streamstore.add_entry(stream_key, id, entry_map)
        response = Server.Protocol.pack(result) |> IO.iodata_to_binary()
        write_line(response, client)
      {:error, message} ->
        error_response = "-ERR #{message}\r\n"
        write_line(error_response, client)
    end
  end

  defp execute_command("TYPE", [key], client) do
    cond do
      Server.Streamstore.get_stream(key) != nil ->
        write_line("+stream\r\n", client)
      Server.Store.get_value_or_false(key) != {:error, :not_found} ->
        write_line("+string\r\n", client)
      true ->
        write_line("+none\r\n", client)
    end
  end

  defp execute_command("XRANGE", [stream_key, start, end_id], client) do
    actual_start = if start == "-" do
      case Server.Streamstore.get_first_id(stream_key) do
        {:ok, first_id} -> first_id
        {:error, _} -> "0-0"  # Use a default start if the stream is empty
      end
    else
      start
    end

    actual_end = if end_id == "+" do
      case Server.Streamstore.get_last_id(stream_key) do
        {:ok, last_id} -> last_id
        {:error, _} -> "0.0"
      end
    else
      end_id
    end

    case Server.Streamstore.get_range(stream_key, actual_start, actual_end) do
      {:ok, entries} ->
        Logger.info("Got entries from Streamstore: #{inspect(entries)}")
        response = format_xrange_response(entries)
        Logger.info("Formatted response: #{inspect(response, limit: :infinity)}")
        write_line(response, client)
      {:error, message} ->
        Logger.error("Error in XRANGE: #{message}")
        error_response = "-ERR #{message}\r\n"
        write_line(error_response, client)
    end
  end


  defp execute_command("XREAD", args, client) do
    case Enum.split_while(args, fn arg -> arg != "streams" end) do
      {["block", timeout | _], ["streams" | rest]} ->
        {stream_keys, ids} = Enum.split(rest, div(length(rest), 2))
        execute_xread_blocking(stream_keys, ids, String.to_integer(timeout), client)
      {_, ["streams" | rest]} ->
        {stream_keys, ids} = Enum.split(rest, div(length(rest), 2))
        response = execute_xread_default(stream_keys, ids)
        write_line(response, client)
    end
  end

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
    else
      replica_count = Server.Clientbuffer.get_client_count();
      write_line(":#{replica_count}\r\n", client)
    end

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

  #------------------------------------------------------------------------------
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


  defp process_id(stream_key, "*") do
    generate_full_id(stream_key)
  end

  defp generate_full_id(stream_key) do
    current_time = System.system_time(:millisecond)
    entries = Server.Streamstore.get_stream(stream_key)
    case entries do
      nil ->
        {:ok, "#{current_time}-0"}
      [] ->
        {:ok, "#{current_time}-0"}
      [{last_id, _} | _] ->
        {last_time, last_seq} = parse_id(last_id)
        if current_time > last_time do
          {:ok, "#{current_time}-0"}
        else
          {:ok, "#{current_time}-#{last_seq + 1}"}
        end
    end
  end

  defp process_id(stream_key, id) do
    case String.split(id, "-") do
      [time_str, "*"] ->
        case Integer.parse(time_str) do
          {time, ""} ->
            generate_id(stream_key, time)
          _ ->
            {:error, "Invalid time format"}
        end
      [time_str, seq_str] ->
        # Logger.info("#{time_str}-#{seq_str}")
        validate_explicit_id(stream_key, "#{time_str}-#{seq_str}")
      _ ->
        {:error, "Invalid ID format"}
    end
  end


  defp generate_id(stream_key, time) do
    entries = Server.Streamstore.get_stream(stream_key)
    case entries do
      nil ->
        if time == 0, do: {:ok, "0-1"}, else: {:ok, "#{time}-0"}
      [] ->
        if time == 0, do: {:ok, "0-1"}, else: {:ok, "#{time}-0"}
      [{last_id, _} | _] ->
        {:ok, {last_time, last_seq}} = parse_id(last_id)
        # Logger.info("#{last_time}:#{last_seq}")
        cond do
          time > last_time -> {:ok, "#{time}-0"}
          time == last_time -> {:ok, "#{time}-#{last_seq + 1}"}
          true -> {:error, "The ID specified in XADD is equal or smaller than the target stream top item"}
        end
    end
  end

  defp validate_explicit_id(stream_key, id) do
    case parse_id(id) do
      {:ok, {new_time, new_seq}} ->
        if new_time == 0 and new_seq == 0 do
          {:error, "The ID specified in XADD must be greater than 0-0"}
        else
          entries = Server.Streamstore.get_stream(stream_key)
          # Logger.info("entries: #{inspect(entries)}")
          case entries do
            nil ->
              :ok
            [] ->
              :ok
            [{last_id, _} | _] ->
              case parse_id(last_id) do
                {:ok, {last_time, last_seq}} ->
                  # Logger.info("#{last_time}:#{last_seq}")
                  if new_time > last_time or (new_time == last_time and new_seq > last_seq) do
                    :ok
                  else
                    {:error, "The ID specified in XADD is equal or smaller than the target stream top item"}
                  end
                :error ->
                  {:error, "Invalid last ID format"}
              end
          end
        end
      :error ->
        {:error, "Invalid ID format"}
    end
  end

  defp parse_id(id) do
    case String.split(id, "-") do
      [time_str, seq_str] ->
        case {Integer.parse(time_str), Integer.parse(seq_str)} do
          {{time, ""}, {seq, ""}} -> {:ok, {time, seq}}
          _ -> :error
        end
      _ -> :error
    end
  end

  defp execute_xread_blocking(stream_keys, ids, timeout, client) do
    Logger.info("Executing blocking XREAD with keys: #{inspect(stream_keys)}, ids: #{inspect(ids)}, timeout: #{timeout}")

    Server.Streamstore.set_block_read_active(true, self())

    actual_ids = Enum.map(Enum.zip(stream_keys, ids), fn {key, id} ->
      if id == "$" do
        case Server.Streamstore.get_last_id(key) do
          {:ok, latest_id} -> latest_id
          {:error, _} -> "0-0"
        end
      else
        id
      end
    end)

    result = if timeout == 0 do
      receive do
        {:stream_update, updated_stream_key, id} ->
          Logger.info("Received stream update for key: #{updated_stream_key}, ids: #{inspect(id)}")
          if updated_stream_key in stream_keys do
            Logger.info("Update matches watched stream, executing default XREAD")
            execute_xread_default(stream_keys, actual_ids)
          else
            Logger.info("Update doesn't match watched stream, continuing to block")
            execute_xread_blocking(stream_keys, actual_ids, timeout, client)
          end
      end
    else
      ref = make_ref()
      timer_ref = Process.send_after(self(), {:timeout, ref}, timeout)

      receive do
        {:timeout, ^ref} ->
          Logger.info("XREAD BLOCK timed out")
          "$-1\r\n"

        {:stream_update, updated_stream_key, id} ->
          Logger.info("Received stream update for key: #{updated_stream_key}, ids: #{inspect(id)}")
          Process.cancel_timer(timer_ref)
          if updated_stream_key in stream_keys do
            Logger.info("Update matches watched stream, executing default XREAD")
            execute_xread_default(stream_keys, actual_ids)
          else
            Logger.info("Update doesn't match watched stream, continuing to block")
            execute_xread_blocking(stream_keys, actual_ids, timeout, client)
          end
      end
    end

    Server.Streamstore.set_block_read_active(false)
    write_line(result, client)
  end

  defp execute_xread_default(stream_keys, ids) do
    results = Enum.zip(stream_keys, ids)
    |> Enum.map(fn {stream_key, id} ->
      case Server.Streamstore.get_entries_after(stream_key, id) do
        {:ok, entries} -> {stream_key, entries}
        {:error, _} -> {stream_key, []}
      end
    end)

    format_xread_response(results)
  end


  defp format_xread_response(results) do
    formatted_results = Enum.map(results, fn {stream_key, entries} ->
      formatted_entries = Enum.map(entries, fn {id, data} ->
        [id, Enum.flat_map(data, fn {k, v} -> [k, v] end)]
      end)
      [stream_key, formatted_entries]
    end)

    Server.Protocol.pack(formatted_results)
  end

  defp format_xrange_response(entries) do
    Logger.info("Formatting entries: #{inspect(entries)}")

    formatted_entries = Enum.map(entries, fn {id, entry} ->
      case entry do
        %{} ->
          flattened_entry = Enum.flat_map(entry, fn {k, v} -> [k, v] end)
          Logger.info("Flattened entry: #{inspect([id, flattened_entry])}")
          [id, flattened_entry]
        _ ->
          Logger.warning("Unexpected entry format: #{inspect(entry)}")
          [id, []]
      end
    end)

    Logger.info("Formatted entries: #{inspect(formatted_entries)}")

    packed_response = Server.Protocol.pack(formatted_entries)
    Logger.info("Packed response: #{inspect(packed_response, limit: :infinity)}")

    IO.iodata_to_binary(packed_response)
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

  defp execute_queued_command(command, client) do
    case command do
      ["SET" | args] -> execute_set_command(args, client)
      ["GET" | args] -> execute_get_command(args, client)
      ["INCR" | args] -> execute_incr_command(args, client)
      _ -> "-ERR Unknown command '#{Enum.at(command, 0)}'\r\n"
    end
  end

  defp execute_set_command([key, value | rest], _client) do
    Logger.info("Key: #{key}, Value: #{value}")
    case rest do
      ["PX", time] ->
        time_ms = String.to_integer(time)
        Server.Store.update(key, value, time_ms)
      [] ->
        Server.Store.update(key, value)
    end
    "+OK\r\n"
  end

  defp execute_get_command([key], _client) do
    case Server.Store.get_value_or_false(key) do
      {:ok, value} -> "$#{byte_size(value)}\r\n#{value}\r\n"
      {:error, _} -> "$-1\r\n"
    end
  end

  defp execute_incr_command([key], _client) do
    case Server.Store.get_value_or_false(key) do
      {:ok, value} ->
        case Integer.parse(value) do
          {int_value, _} ->
            increased_value = int_value + 1
            Server.Store.update(key, Integer.to_string(increased_value))
            ":#{increased_value}\r\n"
          :error ->
            "-ERR value is not an integer or out of range\r\n"
        end
      {:error, _} ->
        Server.Store.update(key, "1")
        ":1\r\n"
    end
  end

  defp write_line(line, client) do
    :gen_tcp.send(client, line)
  end


end
