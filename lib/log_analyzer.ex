defmodule LogAnalyzer do
  @moduledoc """
  A real-time log analyzer using Goldrush.
  
  This module demonstrates how to use Goldrush to process and analyze log events
  in real-time, with different handlers for different types of logs.
  """
  
  @doc """
  Starts the log analyzer.
  
  ## Returns
  
  `:ok`
  """
  def start do
    GoldrushEx.start()
    
    # Compile queries for different log levels
    compile_error_handler()
    compile_warning_handler()
    compile_info_handler()
    compile_debug_handler()
    
    # Compile a query for security-related logs
    compile_security_handler()
    
    # Compile a query for performance-related logs
    compile_performance_handler()
    
    # Compile a query for all logs (for statistics)
    compile_stats_handler()
    
    :ok
  end
  
  @doc """
  Processes a log event.
  
  ## Parameters
  
  - `log_event`: A map containing log data
  
  ## Returns
  
  `:ok`
  """
  def process_log(log_event) when is_map(log_event) do
    # Convert map to keyword list
    event_data = Map.to_list(log_event)
    process_log(event_data)
  end
  
  def process_log(log_event) when is_list(log_event) do
    # Create an event
    event = GreEx.make(log_event)
    
    # Process the event with all handlers
    GoldrushEx.handle(:log_error_handler, event)
    GoldrushEx.handle(:warning_handler, event)
    GoldrushEx.handle(:info_handler, event)
    GoldrushEx.handle(:debug_handler, event)
    GoldrushEx.handle(:security_handler, event)
    GoldrushEx.handle(:performance_handler, event)
    GoldrushEx.handle(:stats_handler, event)
    
    :ok
  end
  
  @doc """
  Gets statistics about processed logs.
  
  ## Returns
  
  A map of statistics
  """
  def get_stats do
    %{
      total: GoldrushEx.input(:stats_handler),
      errors: GoldrushEx.output(:log_error_handler),
      warnings: GoldrushEx.output(:warning_handler),
      info: GoldrushEx.output(:info_handler),
      debug: GoldrushEx.output(:debug_handler),
      security: GoldrushEx.output(:security_handler),
      performance: GoldrushEx.output(:performance_handler)
    }
  end
  
  @doc """
  Resets all statistics.
  
  ## Returns
  
  `:ok`
  """
  def reset_stats do
    GoldrushEx.reset_counters(:log_error_handler)
    GoldrushEx.reset_counters(:warning_handler)
    GoldrushEx.reset_counters(:info_handler)
    GoldrushEx.reset_counters(:debug_handler)
    GoldrushEx.reset_counters(:security_handler)
    GoldrushEx.reset_counters(:performance_handler)
    GoldrushEx.reset_counters(:stats_handler)
    :ok
  end
  
  @doc """
  Stops the log analyzer and cleans up resources.
  
  ## Returns
  
  `:ok`
  """
  def stop do
    # Use try/catch to handle any errors during termination
    try do
      GoldrushEx.delete(:log_error_handler)
      GoldrushEx.delete(:warning_handler)
      GoldrushEx.delete(:info_handler)
      GoldrushEx.delete(:debug_handler)
      GoldrushEx.delete(:security_handler)
      GoldrushEx.delete(:performance_handler)
      GoldrushEx.delete(:stats_handler)
    catch
      _, _ -> :ok
    end
    :ok
  end
  
  # Private functions
  
  defp compile_error_handler do
    query = GoldrushEx.with(
      GoldrushEx.eq(:level, :error),
      fn event ->
        message = GreEx.fetch(:message, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        IO.puts("#{IO.ANSI.red()}[ERROR] [#{timestamp}] #{message}#{IO.ANSI.reset()}")
        
        # You could also send to a monitoring service
        # MonitoringService.report_error(message)
      end
    )
    
    GoldrushEx.compile(:log_error_handler, query, [statistics: true])
  end
  
  defp compile_warning_handler do
    query = GoldrushEx.with(
      GoldrushEx.eq(:level, :warning),
      fn event ->
        # Check if message exists
        message = case GreEx.find(:message, event) do
          {true, msg} -> msg
          false -> "No message provided"
        end
        
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        
        IO.puts("#{IO.ANSI.yellow()}[WARNING] [#{timestamp}] #{message}#{IO.ANSI.reset()}")
      end
    )
    
    GoldrushEx.compile(:warning_handler, query, [statistics: true])
  end
  
  defp compile_info_handler do
    query = GoldrushEx.with(
      GoldrushEx.eq(:level, :info),
      fn event ->
        message = GreEx.fetch(:message, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        IO.puts("#{IO.ANSI.green()}[INFO] [#{timestamp}] #{message}#{IO.ANSI.reset()}")
      end
    )
    
    GoldrushEx.compile(:info_handler, query, [statistics: true])
  end
  
  defp compile_debug_handler do
    query = GoldrushEx.with(
      GoldrushEx.eq(:level, :debug),
      fn event ->
        message = GreEx.fetch(:message, event)
        timestamp = case GreEx.find(:timestamp, event) do
          {true, ts} -> ts
          false -> "unknown time"
        end
        IO.puts("#{IO.ANSI.blue()}[DEBUG] [#{timestamp}] #{message}#{IO.ANSI.reset()}")
      end
    )
    
    GoldrushEx.compile(:debug_handler, query, [statistics: true])
  end
  
  defp compile_security_handler do
    # Complex query for security-related logs
    query = GoldrushEx.with(
      GoldrushEx.any([
        GoldrushEx.all([
          GoldrushEx.wc(:user_id),
          GoldrushEx.any([
            GoldrushEx.eq(:action, :login_failed),
            GoldrushEx.eq(:action, :permission_denied),
            GoldrushEx.eq(:action, :suspicious_activity)
          ])
        ]),
        GoldrushEx.all([
          GoldrushEx.wc(:ip_address),
          GoldrushEx.gt(:failed_attempts, 3)
        ])
      ]),
      fn event ->
        # Extract relevant information
        {user_id, ip} = case {GreEx.find(:user_id, event), GreEx.find(:ip_address, event)} do
          {{true, uid}, {true, ip_addr}} -> {uid, ip_addr}
          {{true, uid}, _} -> {uid, "unknown"}
          {_, {true, ip_addr}} -> {"unknown", ip_addr}
          _ -> {"unknown", "unknown"}
        end
        
        action = case GreEx.find(:action, event) do
          {true, act} -> act
          _ -> "unknown"
        end
        
        # Log security event
        IO.puts("#{IO.ANSI.red()}[SECURITY] User: #{user_id}, IP: #{ip}, Action: #{action}#{IO.ANSI.reset()}")
        
        # You could also trigger alerts or block IPs
        # SecurityService.report_incident(user_id, ip, action)
      end
    )
    
    GoldrushEx.compile(:security_handler, query, [statistics: true])
  end
  
  defp compile_performance_handler do
    # Query for performance-related logs
    query = GoldrushEx.with(
      GoldrushEx.all([
        GoldrushEx.wc(:duration),
        GoldrushEx.gt(:duration, 1000)  # Duration > 1000ms
      ]),
      fn event ->
        # Extract performance information
        duration = GreEx.fetch(:duration, event)
        operation = case GreEx.find(:operation, event) do
          {true, op} -> op
          _ -> "unknown"
        end
        
        # Log slow operation
        IO.puts("#{IO.ANSI.yellow()}[PERFORMANCE] Slow operation: #{operation}, Duration: #{duration}ms#{IO.ANSI.reset()}")
        
        # You could also track performance metrics
        # MetricsService.track_slow_operation(operation, duration)
      end
    )
    
    GoldrushEx.compile(:performance_handler, query, [statistics: true])
  end
  
  defp compile_stats_handler do
    # Query for all logs (for statistics)
    query = GoldrushEx.with(
      GoldrushEx.null(true),  # Match all events
      fn _event ->
        # Just count the event, no action needed
        :ok
      end
    )
    
    GoldrushEx.compile(:stats_handler, query, [statistics: true])
  end
end
