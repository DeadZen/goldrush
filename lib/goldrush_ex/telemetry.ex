defmodule GoldrushEx.Telemetry do
  @moduledoc """
  Telemetry integration for GoldrushEx.
  
  This module provides functions for emitting telemetry events from GoldrushEx.
  """
  
  @doc """
  Emits a telemetry event for event processing.
  """
  def emit_event_processed(handler_name, event, start_time, end_time, metadata \\ %{}) do
    measurements = %{
      duration: end_time - start_time,
      system_time: System.system_time()
    }
    
    :telemetry.execute(
      [:goldrush_ex, :event, :processed],
      measurements,
      Map.merge(%{handler: handler_name, event: event}, metadata)
    )
  end
  
  @doc """
  Emits a telemetry event for event filtering.
  """
  def emit_event_filtered(handler_name, event, start_time, end_time, metadata \\ %{}) do
    measurements = %{
      duration: end_time - start_time,
      system_time: System.system_time()
    }
    
    :telemetry.execute(
      [:goldrush_ex, :event, :filtered],
      measurements,
      Map.merge(%{handler: handler_name, event: event}, metadata)
    )
  end
  
  @doc """
  Emits a telemetry event for job execution.
  """
  def emit_job_executed(handler_name, event, result, execution_time, metadata \\ %{}) do
    measurements = %{
      duration: execution_time,
      system_time: System.system_time()
    }
    
    status = case result do
      {:error, _} -> :error
      _ -> :success
    end
    
    :telemetry.execute(
      [:goldrush_ex, :job, :executed],
      measurements,
      Map.merge(%{handler: handler_name, event: event, result: result, status: status}, metadata)
    )
  end
  
  @doc """
  Emits a telemetry event for plugin processing.
  """
  def emit_plugin_processed(plugin_name, event, start_time, end_time, result, metadata \\ %{}) do
    measurements = %{
      duration: end_time - start_time,
      system_time: System.system_time()
    }
    
    status = case result do
      {:ok, _} -> :success
      {:skip, _} -> :skipped
      _ -> :error
    end
    
    :telemetry.execute(
      [:goldrush_ex, :plugin, :processed],
      measurements,
      Map.merge(%{plugin: plugin_name, event: event, status: status}, metadata)
    )
  end
  
  @doc """
  Attaches a handler to GoldrushEx telemetry events.
  """
  def attach_handler(handler_id, function, events \\ nil) do
    events = events || [
      [:goldrush_ex, :event, :processed],
      [:goldrush_ex, :event, :filtered],
      [:goldrush_ex, :job, :executed],
      [:goldrush_ex, :plugin, :processed]
    ]
    
    :telemetry.attach_many(
      handler_id,
      events,
      function,
      nil
    )
  end
end
