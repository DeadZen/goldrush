defmodule IoTProcessorTest do
  use ExUnit.Case
  import ExUnit.CaptureIO

  setup do
    # Start the processor
    IoTProcessor.start()
    
    # Reset stats before each test
    IoTProcessor.reset_stats()
    
    on_exit(fn ->
      # Clean up after tests
      IoTProcessor.stop()
    end)
    
    :ok
  end

  test "processes temperature events" do
    # Capture output for high temperature
    output = capture_io(fn ->
      IoTProcessor.process_event(%{
        device_id: "device1",
        temperature: 40.0,  # Well above the default threshold
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected alert
    assert output =~ "[TEMPERATURE ALERT]"
    assert output =~ "device1"
    assert output =~ "40.0"
    assert output =~ "high"
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.total == 1
    assert stats.temperature_alerts == 1
  end

  test "processes humidity events" do
    # Capture output for low humidity
    output = capture_io(fn ->
      IoTProcessor.process_event(%{
        device_id: "device1",
        humidity: 10.0,  # Well below the default threshold
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected alert
    assert output =~ "[HUMIDITY ALERT]"
    assert output =~ "device1"
    assert output =~ "10.0"
    assert output =~ "low"
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.total == 1
    assert stats.humidity_alerts == 1
  end

  test "processes motion events" do
    # Capture output for motion detection
    output = capture_io(fn ->
      IoTProcessor.process_event(%{
        device_id: "device1",
        motion: true,
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected alert
    assert output =~ "[MOTION DETECTED]"
    assert output =~ "device1"
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.total == 1
    assert stats.motion_events == 1
  end

  test "processes battery events" do
    # Capture output for low battery
    output = capture_io(fn ->
      IoTProcessor.process_event(%{
        device_id: "device1",
        battery: 5.0,  # Well below the default threshold
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected alert
    assert output =~ "[BATTERY LOW]"
    assert output =~ "device1"
    assert output =~ "5.0"
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.total == 1
    assert stats.battery_alerts == 1
  end

  test "processes anomaly events" do
    # Capture output for anomaly detection
    output = capture_io(fn ->
      IoTProcessor.process_event(%{
        device_id: "device1",
        temperature: 200.0,  # Way above any reasonable value
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected alert
    assert output =~ "[ANOMALY DETECTED]"
    assert output =~ "device1"
    assert output =~ "impossible temperature"
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.total == 1
    assert stats.anomalies == 1
  end

  test "processes status events" do
    # Capture output for status change
    output = capture_io(fn ->
      IoTProcessor.process_event(%{
        device_id: "device1",
        status: :offline,
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Check that the output contains the expected alert
    assert output =~ "[STATUS CHANGE]"
    assert output =~ "device1"
    assert output =~ "OFFLINE"
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.total == 1
    assert stats.status_changes == 1
  end

  test "analyzes historical data" do
    # Sample historical data
    data = [
      %{device_id: "device1", temperature: 25.0, timestamp: "2023-01-01T12:00:00Z"},
      %{device_id: "device1", temperature: 26.0, timestamp: "2023-01-01T12:05:00Z"},
      %{device_id: "device1", temperature: 24.0, timestamp: "2023-01-01T12:10:00Z"},
      %{device_id: "device1", humidity: 50.0, timestamp: "2023-01-01T12:15:00Z"},
      %{device_id: "device1", humidity: 52.0, timestamp: "2023-01-01T12:20:00Z"}
    ]
    
    # Run analysis
    {time, result} = IoTProcessor.analyze_historical_data("device1", data)
    
    # Check result
    assert is_number(time)
    assert result.device_id == "device1"
    assert result.samples == 5
    assert_in_delta result.temperature.avg, 25.0, 0.1
    assert result.temperature.min == 24.0
    assert result.temperature.max == 26.0
    assert_in_delta result.humidity.avg, 51.0, 0.1
    assert result.humidity.min == 50.0
    assert result.humidity.max == 52.0
    
    # Check stats
    stats = IoTProcessor.get_stats()
    assert stats.analysis_jobs == 1
  end

  test "updates configuration" do
    # Initial config
    original_threshold = 30.0
    
    # Update config
    IoTProcessor.update_config(:temp_high_threshold, 35.0)
    
    # Test that high temperature is now only triggered above 35
    output = capture_io(fn ->
      # This would have triggered an alert with the original threshold
      IoTProcessor.process_event(%{
        device_id: "device1",
        temperature: 32.0,
        timestamp: "2023-01-01T12:00:00Z"
      })
    end)
    
    # Should not trigger an alert
    refute output =~ "[TEMPERATURE ALERT]"
    
    # Reset config
    IoTProcessor.update_config(:temp_high_threshold, original_threshold)
  end

  test "resets statistics" do
    # Generate some events
    capture_io(fn ->
      IoTProcessor.process_event(%{device_id: "device1", temperature: 35.0})
      IoTProcessor.process_event(%{device_id: "device1", humidity: 15.0})
    end)
    
    # Check stats before reset
    stats_before = IoTProcessor.get_stats()
    assert stats_before.total == 2
    
    # Reset stats
    IoTProcessor.reset_stats()
    
    # Check stats after reset
    stats_after = IoTProcessor.get_stats()
    assert stats_after.total == 0
    assert stats_after.temperature_alerts == 0
    assert stats_after.humidity_alerts == 0
  end
end
