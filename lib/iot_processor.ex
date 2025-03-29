defmodule Goldrush.IoTProcessor do
  @moduledoc """
  A real-time IoT event processor using Goldrush.
  
  This module demonstrates how to use Goldrush to process and analyze IoT sensor
  data in real-time, with different handlers for different types of events.
  """
  
  @doc """
  Starts the IoT processor.
  
  ## Parameters
  
  - `config`: Configuration options (optional)
  
  ## Returns
  
  `:ok`
  """
  def start(config \\ %{}) do
    GoldrushEx.start()
    
    # Store configuration for later use
    store = Map.to_list(Map.merge(default_config(), config))
    
    # Compile queries for different sensor types
    compile_temperature_handler(store)
    compile_humidity_handler(store)
    compile_motion_handler(store)
    compile_battery_handler(store)
    
    # Compile a query for anomaly detection
    compile_anomaly_handler(store)
    
    # Compile a query for device status
    compile_status_handler(store)
    
    # Compile a query for all events (for statistics)
    compile_stats_handler()
    
    :ok
  end
  
  @doc """
  Processes an IoT event.
  
  ## Parameters
  
  - `event_data`: A map containing sensor data
  
  ## Returns
  
  `:ok`
  """
  def process_event(event_data) when is_map(event_data) do
    # Convert map to keyword list
    event_list = Map.to_list(event_data)
    process_event(event_list)
  end
  
  def process_event(event_data) when is_list(event_data) do
    # Add timestamp if not present
    event_data = 
      if Keyword.has_key?(event_data, :timestamp) do
        event_data
      else
        [{:timestamp, DateTime.utc_now()} | event_data]
      end
    
    # Create an event
    event = GreEx.make(event_data)
    
    # Debug output
    IO.puts("Processing event: #{inspect(event_data)}")
    
    # Process the event with all handlers
    GoldrushEx.handle(:temperature_handler, event)
    GoldrushEx.handle(:humidity_handler, event)
    GoldrushEx.handle(:motion_handler, event)
    GoldrushEx.handle(:battery_handler, event)
    GoldrushEx.handle(:anomaly_handler, event)
    GoldrushEx.handle(:status_handler, event)
    GoldrushEx.handle(:stats_handler, event)
    
    # Debug output
    stats = get_stats()
    IO.puts("Stats after processing: #{inspect(stats)}")
    
    :ok
  end
  
  @doc """
  Runs a job to analyze historical data.
  
  ## Parameters
  
  - `device_id`: The device ID to analyze
  - `data`: Historical data to analyze
  
  ## Returns
  
  `{execution_time, result}`
  """
  def analyze_historical_data(device_id, data) do
    # Create an event with the device ID
    event = GreEx.make([{:device_id, device_id}])
    
    # Run the analysis job
    GoldrushEx.run(:stats_handler, fn _event, _store ->
      # Simulate complex analysis
      Process.sleep(100)
      
      # Calculate average, min, max values
      temps = Enum.filter(data, fn d -> Map.has_key?(d, :temperature) end)
      humids = Enum.filter(data, fn d -> Map.has_key?(d, :humidity) end)
      
      temp_stats = if length(temps) > 0 do
        temp_values = Enum.map(temps, & &1.temperature)
        %{
          avg: Enum.sum(temp_values) / length(temp_values),
          min: Enum.min(temp_values),
          max: Enum.max(temp_values)
        }
      else
        nil
      end
      
      humid_stats = if length(humids) > 0 do
        humid_values = Enum.map(humids, & &1.humidity)
        %{
          avg: Enum.sum(humid_values) / length(humid_values),
          min: Enum.min(humid_values),
          max: Enum.max(humid_values)
        }
      else
        nil
      end
      
      %{
        device_id: device_id,
        samples: length(data),
        temperature: temp_stats,
        humidity: humid_stats
      }
    end, event)
  end
  
  @doc """
  Gets statistics about processed events.
  
  ## Returns
  
  A map of statistics
  """
  def get_stats do
    %{
      total: GoldrushEx.input(:stats_handler),
      temperature_alerts: GoldrushEx.output(:temperature_handler),
      humidity_alerts: GoldrushEx.output(:humidity_handler),
      motion_events: GoldrushEx.output(:motion_handler),
      battery_alerts: GoldrushEx.output(:battery_handler),
      anomalies: GoldrushEx.output(:anomaly_handler),
      status_changes: GoldrushEx.output(:status_handler),
      analysis_jobs: GoldrushEx.job_run(:stats_handler),
      analysis_errors: GoldrushEx.job_error(:stats_handler),
      avg_analysis_time_ms: avg_analysis_time()
    }
  end
  
  @doc """
  Updates configuration settings.
  
  ## Parameters
  
  - `key`: The configuration key to update
  - `value`: The new value
  
  ## Returns
  
  `:ok`
  """
  def update_config(key, value) do
    # Get current config from handlers
    {:ok, current_config} = GoldrushEx.get(:temperature_handler, :config)
    
    # Update the config
    new_config = Keyword.put(current_config, key, value)
    
    # Recompile handlers with new config
    start(Map.new(new_config))
    
    :ok
  end
  
  @doc """
  Resets all statistics.
  
  ## Returns
  
  `:ok`
  """
  def reset_stats do
    GoldrushEx.reset_counters(:temperature_handler)
    GoldrushEx.reset_counters(:humidity_handler)
    GoldrushEx.reset_counters(:motion_handler)
    GoldrushEx.reset_counters(:battery_handler)
    GoldrushEx.reset_counters(:anomaly_handler)
    GoldrushEx.reset_counters(:status_handler)
    GoldrushEx.reset_counters(:stats_handler)
    :ok
  end
  
  @doc """
  Stops the IoT processor and cleans up resources.
  
  ## Returns
  
  `:ok`
  """
  def stop do
    # Use try/catch to handle any errors during termination
    try do
      GoldrushEx.delete(:temperature_handler)
      GoldrushEx.delete(:humidity_handler)
      GoldrushEx.delete(:motion_handler)
      GoldrushEx.delete(:battery_handler)
      GoldrushEx.delete(:anomaly_handler)
      GoldrushEx.delete(:status_handler)
      GoldrushEx.delete(:stats_handler)
    catch
      _, _ -> :ok
    end
    :ok
  end
  
  # Private functions
  
  defp default_config do
    %{
      temp_high_threshold: 30.0,
      temp_low_threshold: 5.0,
      humidity_high_threshold: 80.0,
      humidity_low_threshold: 20.0,
      battery_low_threshold: 15.0,
      anomaly_threshold: 3.0
    }
  end
  
  defp avg_analysis_time do
    job_time = GoldrushEx.job_time(:stats_handler)
    job_count = GoldrushEx.job_run(:stats_handler)
    
    if job_count > 0 do
      job_time / job_count / 1000  # Convert to milliseconds
    else
      0
    end
  end
  
  defp compile_temperature_handler(config) do
    query = GoldrushEx.with(
      GoldrushEx.all([
        GoldrushEx.wc(:temperature),
        GoldrushEx.any([
          GoldrushEx.gt(:temperature, Keyword.get(config, :temp_high_threshold)),
          GoldrushEx.lt(:temperature, Keyword.get(config, :temp_low_threshold))
        ])
      ]),
      fn event, _store ->
        # Extract data
        device_id = GreEx.fetch(:device_id, event)
        temperature = GreEx.fetch(:temperature, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        # Determine alert type
        alert_type = cond do
          temperature > Keyword.get(config, :temp_high_threshold) -> "high"
          temperature < Keyword.get(config, :temp_low_threshold) -> "low"
          true -> "unknown"
        end
        
        # Log alert
        IO.puts("#{IO.ANSI.red()}[TEMPERATURE ALERT] Device: #{device_id}, Temperature: #{temperature}°C (#{alert_type}), Time: #{timestamp}#{IO.ANSI.reset()}")
        
        # You could also send alerts
        # AlertService.send_temperature_alert(device_id, temperature, alert_type)
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:temperature_handler, query, [config: config, statistics: true])
  end
  
  defp compile_humidity_handler(config) do
    query = GoldrushEx.with(
      GoldrushEx.all([
        GoldrushEx.wc(:humidity),
        GoldrushEx.any([
          GoldrushEx.gt(:humidity, Keyword.get(config, :humidity_high_threshold)),
          GoldrushEx.lt(:humidity, Keyword.get(config, :humidity_low_threshold))
        ])
      ]),
      fn event, _store ->
        # Extract data
        device_id = GreEx.fetch(:device_id, event)
        humidity = GreEx.fetch(:humidity, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        # Determine alert type
        alert_type = cond do
          humidity > Keyword.get(config, :humidity_high_threshold) -> "high"
          humidity < Keyword.get(config, :humidity_low_threshold) -> "low"
          true -> "unknown"
        end
        
        # Log alert
        IO.puts("#{IO.ANSI.yellow()}[HUMIDITY ALERT] Device: #{device_id}, Humidity: #{humidity}% (#{alert_type}), Time: #{timestamp}#{IO.ANSI.reset()}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:humidity_handler, query, [config: config, statistics: true])
  end
  
  defp compile_motion_handler(_config) do
    query = GoldrushEx.with(
      GoldrushEx.all([
        GoldrushEx.wc(:motion),
        GoldrushEx.eq(:motion, true)
      ]),
      fn event, _store ->
        # Extract data
        device_id = GreEx.fetch(:device_id, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        # Log motion event
        IO.puts("#{IO.ANSI.blue()}[MOTION DETECTED] Device: #{device_id}, Time: #{timestamp}#{IO.ANSI.reset()}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:motion_handler, query, [statistics: true])
  end
  
  defp compile_battery_handler(config) do
    query = GoldrushEx.with(
      GoldrushEx.all([
        GoldrushEx.wc(:battery),
        GoldrushEx.lt(:battery, Keyword.get(config, :battery_low_threshold))
      ]),
      fn event, _store ->
        # Extract data
        device_id = GreEx.fetch(:device_id, event)
        battery = GreEx.fetch(:battery, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        # Log battery alert
        IO.puts("#{IO.ANSI.red()}[BATTERY LOW] Device: #{device_id}, Battery: #{battery}%, Time: #{timestamp}#{IO.ANSI.reset()}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:battery_handler, query, [config: config, statistics: true])
  end
  
  defp compile_anomaly_handler(config) do
    # Simplified query for anomaly detection - just check for impossible temperature
    query = GoldrushEx.with(
      GoldrushEx.gt(:temperature, 100),  # Impossibly high temperature
      fn event, _store ->
        # Extract data
        device_id = GreEx.fetch(:device_id, event)
        temperature = GreEx.fetch(:temperature, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        # Log anomaly
        IO.puts("#{IO.ANSI.red()}[ANOMALY DETECTED] Device: #{device_id}, Temperature: #{temperature}°C (impossible temperature), Time: #{timestamp}#{IO.ANSI.reset()}")
        
        # You could also trigger further investigation
        # AnomalyService.investigate(device_id, "impossible temperature", event)
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:anomaly_handler, query, [config: config, statistics: true])
  end
  
  defp compile_status_handler(_config) do
    query = GoldrushEx.with(
      GoldrushEx.all([
        GoldrushEx.wc(:status),
        GoldrushEx.any([
          GoldrushEx.eq(:status, :online),
          GoldrushEx.eq(:status, :offline),
          GoldrushEx.eq(:status, :error)
        ])
      ]),
      fn event, _store ->
        # Extract data
        device_id = GreEx.fetch(:device_id, event)
        status = GreEx.fetch(:status, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        # Format based on status
        {color, status_str} = case status do
          :online -> {IO.ANSI.green(), "ONLINE"}
          :offline -> {IO.ANSI.yellow(), "OFFLINE"}
          :error -> {IO.ANSI.red(), "ERROR"}
          _ -> {IO.ANSI.white(), "UNKNOWN"}
        end
        
        # Log status change
        IO.puts("#{color}[STATUS CHANGE] Device: #{device_id}, Status: #{status_str}, Time: #{timestamp}#{IO.ANSI.reset()}")
        
        # You could also update a device registry
        # DeviceRegistry.update_status(device_id, status)
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:status_handler, query, [statistics: true])
  end
  
  defp compile_stats_handler do
    # Query for all events (for statistics)
    query = GoldrushEx.with(
      GoldrushEx.null(true),  # Match all events
      fn _event ->
        # Just count the event, no action needed
        :ok
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:stats_handler, query, [statistics: true])
  end
end
