defmodule GoldrushEx.Plugins.CloudEvents do
  @moduledoc """
  A plugin that converts events to and from CloudEvents format.
  
  This plugin implements the CloudEvents specification (https://cloudevents.io/)
  for standardized event formatting.
  """
  
  @behaviour GoldrushEx.Plugin
  
  @cloud_event_version "1.0"
  
  @impl true
  def init(opts) do
    config = %{
      source: Keyword.get(opts, :source, "goldrush-ex"),
      type_prefix: Keyword.get(opts, :type_prefix, "io.goldrush."),
      include_metadata: Keyword.get(opts, :include_metadata, true),
      validate_events: Keyword.get(opts, :validate_events, true)
    }
    
    {:ok, config}
  end
  
  @impl true
  def handle_event(event, state) do
    # Check if this is already a CloudEvent
    if is_cloud_event?(event) do
      # Already a CloudEvent, pass it through
      {:ok, event}
    else
      # Convert to CloudEvent format
      cloud_event = to_cloud_event(event, state)
      {:ok, cloud_event}
    end
  end
  
  @impl true
  def terminate(_reason, _state) do
    :ok
  end
  
  # Private functions
  
  defp is_cloud_event?(event) do
    GoldrushEx.EventHelper.is_cloud_event?(event)
  end
  
  defp to_cloud_event(event, config) do
    # Extract event type if available, or use default
    event_type = case GreEx.find(:event_type, event) do
      {true, type} -> "#{config.type_prefix}#{type}"
      false -> 
        case GreEx.find(:level, event) do
          {true, level} -> "#{config.type_prefix}#{level}"
          false -> "#{config.type_prefix}event"
        end
    end
    
    # Generate a unique ID if not present
    event_id = case GreEx.find(:id, event) do
      {true, id} -> id
      false -> generate_id()
    end
    
    # Get timestamp or use current time
    timestamp = case GreEx.find(:timestamp, event) do
      {true, ts} -> format_timestamp(ts)
      false -> format_timestamp(DateTime.utc_now())
    end
    
    # Create the CloudEvent
    cloud_event_fields = [
      specversion: @cloud_event_version,
      id: event_id,
      source: config.source,
      type: event_type,
      time: timestamp,
      datacontenttype: "application/json"
    ]
    
    # Add data field with the original event fields
    data = Enum.reduce(GreEx.pairs(event), %{}, fn {k, v}, acc ->
      # Skip fields that are now in the CloudEvent envelope
      if k not in [:specversion, :id, :source, :type, :time, :datacontenttype] do
        Map.put(acc, k, v)
      else
        acc
      end
    end)
    
    cloud_event_fields = cloud_event_fields ++ [data: data]
    
    # Create the CloudEvent
    GreEx.make(cloud_event_fields)
  end
  
  defp generate_id do
    <<part1::32, part2::16, part3::16, part4::16, part5::48>> = :crypto.strong_rand_bytes(16)
    format = ~c"~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b"
    :io_lib.format(format, [part1, part2, part3, part4, part5])
    |> List.to_string()
  end
  
  defp format_timestamp(%DateTime{} = dt) do
    DateTime.to_iso8601(dt)
  end
  
  defp format_timestamp(timestamp) when is_binary(timestamp) do
    timestamp
  end
  
  defp format_timestamp(_) do
    DateTime.to_iso8601(DateTime.utc_now())
  end
end
