defmodule Goldrush.LogAnalyzerTest do
  use ExUnit.Case
  import ExUnit.CaptureIO

  alias Goldrush.LogAnalyzer

  setup do
    # Start the analyzer
    LogAnalyzer.start()
    
    # Reset stats before each test
    LogAnalyzer.reset_stats()
    
    on_exit(fn ->
      # Clean up after tests
      LogAnalyzer.stop()
    end)
    
    :ok
  end

  test "processes error logs" do
    # Capture output for error log
    output = capture_io(fn ->
      LogAnalyzer.process_log(%{
        level: :error,
        message: "Database connection failed",
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected message
    assert output =~ "[ERROR]"
    assert output =~ "Database connection failed"
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 1
    assert stats.errors == 1
  end

  test "processes warning logs" do
    # Capture output for warning log
    output = capture_io(fn ->
      LogAnalyzer.process_log(%{
        level: :warning,
        message: "High memory usage detected",
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected message
    assert output =~ "[WARNING]"
    assert output =~ "High memory usage detected"
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 1
    assert stats.warnings == 1
  end

  test "processes info logs" do
    # Capture output for info log
    output = capture_io(fn ->
      LogAnalyzer.process_log(%{
        level: :info,
        message: "Application started",
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected message
    assert output =~ "[INFO]"
    assert output =~ "Application started"
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 1
    assert stats.info == 1
  end

  test "processes debug logs" do
    # Capture output for debug log
    output = capture_io(fn ->
      LogAnalyzer.process_log(%{
        level: :debug,
        message: "Processing request",
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected message
    assert output =~ "[DEBUG]"
    assert output =~ "Processing request"
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 1
    assert stats.debug == 1
  end

  test "processes security logs" do
    # Capture output for security log
    output = capture_io(fn ->
      LogAnalyzer.process_log(%{
        level: :warning,
        user_id: "user123",
        action: :login_failed,
        ip_address: "192.168.1.1",
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected message
    assert output =~ "[SECURITY]"
    assert output =~ "user123"
    assert output =~ "192.168.1.1"
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 1
    assert stats.security == 1
  end

  test "processes performance logs" do
    # Capture output for performance log
    output = capture_io(fn ->
      LogAnalyzer.process_log(%{
        level: :warning,
        operation: "database_query",
        duration: 1500,
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected message
    assert output =~ "[PERFORMANCE]"
    assert output =~ "database_query"
    assert output =~ "1500ms"
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 1
    assert stats.performance == 1
  end

  test "resets statistics" do
    # Generate some logs
    capture_io(fn ->
      LogAnalyzer.process_log(%{level: :error, message: "Error 1"})
      LogAnalyzer.process_log(%{level: :warning, message: "Warning 1"})
    end)
    
    # Check stats before reset
    stats_before = LogAnalyzer.get_stats()
    assert stats_before.total == 2
    
    # Reset stats
    LogAnalyzer.reset_stats()
    
    # Check stats after reset
    stats_after = LogAnalyzer.get_stats()
    assert stats_after.total == 0
    assert stats_after.errors == 0
    assert stats_after.warnings == 0
  end

  test "processes multiple log types" do
    # Generate mixed logs
    capture_io(fn ->
      LogAnalyzer.process_log(%{level: :error, message: "Error 1"})
      LogAnalyzer.process_log(%{level: :warning, message: "Warning 1"})
      LogAnalyzer.process_log(%{level: :info, message: "Info 1"})
      LogAnalyzer.process_log(%{level: :debug, message: "Debug 1"})
    end)
    
    # Check stats
    stats = LogAnalyzer.get_stats()
    assert stats.total == 4
    assert stats.errors == 1
    assert stats.warnings == 1
    assert stats.info == 1
    assert stats.debug == 1
  end
end
