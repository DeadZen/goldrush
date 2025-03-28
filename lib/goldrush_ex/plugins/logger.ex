defmodule GoldrushEx.Plugins.Logger do
  @moduledoc """
  A plugin that logs all events to the Elixir Logger.
  """
  
  @behaviour GoldrushEx.Plugin
  
  require Logger
  
  @impl true
  def init(opts) do
    log_level = Keyword.get(opts, :level, :debug)
    {:ok, %{level: log_level}}
  end
  
  @impl true
  def handle_event(event, %{level: level}) do
    # Extract event data
    message = GoldrushEx.Event.get(event, :message, "No message")
    event_level = GoldrushEx.Event.get(event, :level, :info)
    
    # Log the event
    Logger.log(level, "[GoldrushEx] [#{event_level}] #{message}")
    
    # Pass the event through unchanged
    {:ok, event}
  end
  
  @impl true
  def terminate(_reason, _state) do
    :ok
  end
end
