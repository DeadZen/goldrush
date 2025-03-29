defmodule GoldrushEx.EventHelper do
  @moduledoc """
  Helper functions for working with events, including CloudEvents.
  """
  
  @doc """
  Gets a field from an event, handling both regular events and CloudEvents.
  
  For CloudEvents, checks the data field for the requested field.
  For the special case of :event_type, also checks the type field.
  """
  def get_field(event, field, default \\ nil) do
    # Special handling for event_type with CloudEvents
    if field == :event_type and is_cloud_event?(event) do
      # For CloudEvents, extract event_type from the type field
      case GreEx.find(:type, event) do
        {true, type} ->
          # Extract the event type from the type field (e.g., "io.example.user_login" -> :user_login)
          type_parts = String.split(type, ".")
          if length(type_parts) > 0 do
            String.to_existing_atom(List.last(type_parts))
          else
            default
          end
        _ -> 
          # Fallback to checking the data field
          get_from_data(event, field, default)
      end
    else
      # Regular field lookup
      if is_cloud_event?(event) do
        get_from_data(event, field, default)
      else
        case GreEx.find(field, event) do
          {true, value} -> value
          _ -> default
        end
      end
    end
  end
  
  @doc """
  Checks if an event matches a condition, handling both regular events and CloudEvents.
  """
  def matches_condition?(event, {field, value}) do
    # Simple field=value condition
    get_field(event, field) == value
  end
  
  def matches_condition?(event, {field, operator, value}) do
    # Field operator value condition
    field_value = get_field(event, field)
    
    case operator do
      :eq -> field_value == value
      :neq -> field_value != value
      :gt -> field_value > value
      :gte -> field_value >= value
      :lt -> field_value < value
      :lte -> field_value <= value
      :in -> field_value in value
      :contains -> 
        if is_binary(field_value) and is_binary(value) do
          String.contains?(field_value, value)
        else
          false
        end
      :starts_with -> 
        if is_binary(field_value) and is_binary(value) do
          String.starts_with?(field_value, value)
        else
          false
        end
      :ends_with -> 
        if is_binary(field_value) and is_binary(value) do
          String.ends_with?(field_value, value)
        else
          false
        end
      _ -> false
    end
  end
  
  def matches_condition?(event, condition) when is_function(condition) do
    # Function condition
    condition.(event)
  end
  
  @doc """
  Checks if an event is a CloudEvent.
  """
  def is_cloud_event?(event) do
    GreEx.has(:specversion, event) and
    GreEx.has(:source, event) and
    GreEx.has(:id, event) and
    GreEx.has(:type, event)
  end
  
  # Private functions
  
  defp get_from_data(event, field, default) do
    # Get the field from the data field of a CloudEvent
    case GreEx.find(:data, event) do
      {true, data} when is_map(data) ->
        Map.get(data, field, default)
      _ -> default
    end
  end
end
