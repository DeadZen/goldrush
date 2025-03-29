defmodule GoldrushEx.Plugins.Router do
  @moduledoc """
  A plugin that routes events based on their properties.
  
  This plugin allows for dynamic routing of events to different handlers
  based on event properties like type, source, or content.
  """
  
  @behaviour GoldrushEx.Plugin
  
  @impl true
  def init(opts) do
    routes = Keyword.get(opts, :routes, [])
    default_handler = Keyword.get(opts, :default_handler)
    
    state = %{
      routes: routes,
      default_handler: default_handler
    }
    
    {:ok, state}
  end
  
  @impl true
  def handle_event(event, state) do
    # Find matching routes
    matching_routes = Enum.filter(state.routes, fn {condition, _handler} ->
      matches_condition?(event, condition)
    end)
    
    case matching_routes do
      [] ->
        # No matching routes, use default handler if available
        if state.default_handler do
          route_to_handler(event, state.default_handler)
        else
          # No default handler, pass through unchanged
          {:ok, event}
        end
        
      routes ->
        # Route to all matching handlers
        Enum.each(routes, fn {_condition, handler} ->
          route_to_handler(event, handler)
        end)
        
        # Mark as processed but don't modify the event
        {:ok, event}
    end
  end
  
  @impl true
  def terminate(_reason, _state) do
    :ok
  end
  
  # Private functions
  
  defp matches_condition?(event, condition) when is_function(condition) do
    # Function condition
    condition.(event)
  end
  
  defp matches_condition?(event, {field, value}) do
    # Simple field=value condition
    case GreEx.find(field, event) do
      {true, ^value} -> true
      _ -> false
    end
  end
  
  defp matches_condition?(event, {field, operator, value}) do
    # Field operator value condition
    case GreEx.find(field, event) do
      {true, field_value} ->
        case operator do
          :eq -> field_value == value
          :neq -> field_value != value
          :gt -> field_value > value
          :gte -> field_value >= value
          :lt -> field_value < value
          :lte -> field_value <= value
          :in -> field_value in value
          :contains -> String.contains?(to_string(field_value), to_string(value))
          :starts_with -> String.starts_with?(to_string(field_value), to_string(value))
          :ends_with -> String.ends_with?(to_string(field_value), to_string(value))
          _ -> false
        end
      _ -> false
    end
  end
  
  defp matches_condition?(event, conditions) when is_list(conditions) do
    # List of conditions (AND)
    Enum.all?(conditions, fn condition ->
      matches_condition?(event, condition)
    end)
  end
  
  defp matches_condition?(_event, _condition) do
    false
  end
  
  defp route_to_handler(event, handler) when is_atom(handler) do
    # Route to a named handler module
    case :glc.handle(handler, event) do
      :ok -> {:ok, event}
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end
  
  defp route_to_handler(event, {module, function}) when is_atom(module) and is_atom(function) do
    # Route to a specific module function
    apply(module, function, [event])
  end
  
  defp route_to_handler(event, handler) when is_function(handler, 1) do
    # Route to a function
    handler.(event)
  end
  
  defp route_to_handler(_event, _handler) do
    # Invalid handler
    {:error, :invalid_handler}
  end
end
