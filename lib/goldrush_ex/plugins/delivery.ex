defmodule GoldrushEx.Plugins.Delivery do
  @moduledoc """
  A plugin that delivers events to external systems.
  
  This plugin allows for delivery of events to various destinations like
  HTTP endpoints, message queues, or other systems.
  """
  
  @behaviour GoldrushEx.Plugin
  
  alias GoldrushEx.EventHelper
  
  @impl true
  def init(opts) do
    destinations = Keyword.get(opts, :destinations, [])
    retry_strategy = Keyword.get(opts, :retry_strategy, :none)
    batch_size = Keyword.get(opts, :batch_size, 1)
    
    state = %{
      destinations: destinations,
      retry_strategy: retry_strategy,
      batch_size: batch_size,
      batch_buffer: %{},
      last_delivery: %{}
    }
    
    {:ok, state}
  end
  
  @impl true
  def handle_event(event, state) do
    # Deliver to all configured destinations
    Enum.each(state.destinations, fn {name, config} ->
      deliver_event(event, name, config, state)
    end)
    
    # Pass the event through unchanged
    {:ok, event}
  end
  
  @impl true
  def terminate(_reason, _state) do
    :ok
  end
  
  # Private functions
  
  defp deliver_event(event, name, config, state) do
    # Check if this event should be delivered to this destination
    if should_deliver?(event, config) do
      case config.type do
        :http -> deliver_http(event, name, config, state)
        :console -> deliver_console(event, name, config)
        :file -> deliver_file(event, name, config)
        :function -> deliver_function(event, name, config)
        _ -> {:error, :unknown_destination_type}
      end
    else
      {:ok, :skipped}
    end
  end
  
  defp should_deliver?(event, config) do
    case config[:filter] do
      nil -> true
      filter -> EventHelper.matches_condition?(event, filter)
    end
  end
  
  defp deliver_http(event, _name, config, _state) do
    # Convert event to JSON
    body = event_to_json(event)

    url = config.url
    method = config[:method] || :post
    headers = config[:headers] || []

    # Make the HTTP request using Req
    result =
      Req.new(url: url)
      |> Req.Request.put_header("content-type", "application/cloudevents+json")
      |> Req.request(method: method, body: body, headers: headers)

    case result do
      {:ok, %{status: status} = response} when status >= 200 and status < 300 ->
        {:ok, response.body}
      {:ok, %{status: status} = response} ->
        {:error, {:http_error, status, response.body}}
      {:error, reason} ->
        {:error, reason}
    end
  end  
  
  defp deliver_console(event, name, _config) do
    IO.puts("Delivery to #{name}:")
    IO.inspect(event, label: "Event")
    {:ok, :delivered}
  end
  
  defp deliver_file(event, _name, config) do
    # Convert event to string representation
    content = event_to_json(event) <> "\n"
    
    # Append to file
    file = config.path
    File.write(file, content, [:append])
  end
  
  defp deliver_function(event, _name, config) do
    # Call the configured function
    function = config.function
    function.(event)
  end
  
  defp event_to_json(event) do
    # Convert event to map
    event_map = Enum.reduce(GreEx.pairs(event), %{}, fn {k, v}, acc ->
      Map.put(acc, k, v)
    end)
    
    # Convert to JSON
    Jason.encode!(event_map)
  end
end
