defmodule Server.Streamstore do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state) do
    {:ok, state}
  end

  def add_entry(stream_key, id, entry) do
    GenServer.call(__MODULE__, {:add_entry, stream_key, id, entry})
  end

  def get_stream(stream_key) do
    GenServer.call(__MODULE__, {:get_stream, stream_key})
  end

  def get_range(stream_key, start, end_id) do
    GenServer.call(__MODULE__, {:get_range, stream_key, start, end_id})
  end

  def get_first_id(stream_key) do
    GenServer.call(__MODULE__, {:get_first_id, stream_key})
  end

  def handle_call({:get_range, stream_key, start, end_id}, _from, state) do
    case Map.get(state, stream_key) do
      nil ->
        {:reply, {:ok, []}, state}
      entries ->
        filtered_entries = filter_entries(entries, start, end_id)
        {:reply, {:ok, filtered_entries}, state}
    end
  end

  def handle_call({:add_entry, stream_key, id, entry}, _from, state) do
    new_state = Map.update(state, stream_key, [{id, entry}], fn entries ->
      [{id, entry} | entries]
    end)
    {:reply, id, new_state}
  end

  def handle_call({:get_stream, stream_key}, _from, state) do
    {:reply, Map.get(state, stream_key), state}
  end

  def handle_call({:get_first_id, stream_key}, _from, state) do
    case Map.get(state, stream_key) do
      nil ->
        {:reply, {:error, :stream_not_found}, state}
      stream when is_list(stream) ->
        case List.last(stream) do
          {id, _} -> {:reply, {:ok, id}, state}
          _ -> {:reply, {:error, :invalid_stream_format}, state}
        end
      _ ->
        {:reply, {:error, :invalid_stream_format}, state}
    end
  end

  defp filter_entries(entries, start, end_id) do
    {start_time, start_seq} = parse_id_with_defaults(start)
    {end_time, end_seq} = parse_id_with_defaults(end_id)

    Enum.filter(entries, fn {id, _} ->
      {time, seq} = parse_id(id)
      (time > start_time || (time == start_time && seq >= start_seq)) &&
      (time < end_time || (time == end_time && seq <= end_seq))
    end)
    |> Enum.reverse()  # Reverse to get ascending order
  end

  defp parse_id_with_defaults(id) do
    case String.split(id, "-") do
      [time_str] -> {String.to_integer(time_str), 0}
      [time_str, seq_str] -> {String.to_integer(time_str), String.to_integer(seq_str)}
      _ -> {0, 0}  # Default case, should not happen with valid input
    end
  end

  defp parse_id(id) do
    [time_str, seq_str] = String.split(id, "-")
    {String.to_integer(time_str), String.to_integer(seq_str)}
  end

end
