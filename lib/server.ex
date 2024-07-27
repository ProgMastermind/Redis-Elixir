defmodule Server do
  @moduledoc """
  Your implementation of a Redis server
  """

  use Application

  def start(_type, _args) do
    port = parse_port()

    children = [
      Server.Store,
      {Task, fn-> Server.listen(port) end}
    ]

    opts = [strategy: :one_for_one, name: :sup]
    Supervisor.start_link(children, opts)
  end

  defp parse_port do
    {opts, _, _} = OptionParser.parse(System.argv(), switches: [port: :integer])
    opts[:port] || 6379
  end

  @doc """
  Listen for incoming connections
  """
  def listen(port) do
    IO.puts("Server listening on port 6379")
    {:ok, socket} = :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true])
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    spawn(fn -> serve(client) end)
    loop_acceptor(socket)
  end

  defp serve(client) do
    client
    |> read_line()
    |> process_command(client)

    serve(client)
  end

  defp read_line(client) do
    {:ok, data} = :gen_tcp.recv(client, 0)
    data
  end

  defp process_command(command, client) do
    IO.puts("Received command: #{inspect(command)}")  # Debug line
    case Server.Protocol.parse(command) do
      {:ok, parsed_data, _rest} ->
        IO.puts("Parsed data: #{inspect(parsed_data)}")  # Debug line
        handle_command(parsed_data, client)
      {:continuation, _fun} ->
        IO.puts("Incomplete command")  # Debug line
        write_line("-ERR Incomplete command\r\n", client)
    end
  end

  defp handle_command(parsed_data, client) do
    case parsed_data do
      [command | args] ->
        execute_command(String.upcase(to_string(command)), args, client)
      _ ->
        write_line("-ERR Invalid command format\r\n", client)
    end
  end

  defp execute_command("ECHO", [message], client) do
    response = Server.Protocol.pack(message) |> IO.iodata_to_binary()
    write_line(response, client)
  end

  defp execute_command("SET", [key, value], client) do
    try do
      Server.Store.update(key, value)
      write_line("+OK\r\n", client)
    catch
      _ ->
        write_line("-ERR Internal server error\r\n", client)
    end
  end

  defp execute_command("SET", [key, value, command, time], client) do
    command = String.upcase(to_string(command))
    if command == "PX" do
      try do
        time_ms = String.to_integer(time)
        Server.Store.update(key, value, time_ms)
        write_line("+OK\r\n", client)
      catch
        _ ->
          write_line("-ERR Internal server error\r\n", client)
      end
    else
      write_line("-ERR Invalid SET command format\r\n", client)
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
