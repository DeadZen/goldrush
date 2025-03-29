defmodule GoldrushEx.CloudEventsTest do
  use ExUnit.Case
  
  setup do
    # Ensure Goldrush is started
    Application.ensure_all_started(:goldrush)
    
    # Register the CloudEvents plugin
    {:ok, plugin_id} = GoldrushEx.Plugin.register(GoldrushEx.Plugins.CloudEvents, [
      source: "test-source",
      type_prefix: "io.test."
    ])
    
    on_exit(fn ->
      # Clean up
      GoldrushEx.Plugin.unregister(plugin_id)
    end)
    
    :ok
  end
  
  test "converts events to CloudEvents format" do
    # Create a regular event
    event = GreEx.make([
      event_type: :test_event,
      message: "Test message",
      value: 42
    ])
    
    # Process through the plugin
    {:ok, cloud_event} = GoldrushEx.Plugin.process_event(event)
    
    # Verify CloudEvents format
    assert GreEx.has(:specversion, cloud_event)
    assert GreEx.has(:source, cloud_event)
    assert GreEx.has(:id, cloud_event)
    assert GreEx.has(:type, cloud_event)
    assert GreEx.has(:time, cloud_event)
    assert GreEx.has(:data, cloud_event)
    
    # Check values
    assert GreEx.fetch(:specversion, cloud_event) == "1.0"
    assert GreEx.fetch(:source, cloud_event) == "test-source"
    assert GreEx.fetch(:type, cloud_event) == "io.test.test_event"
    
    # Check data
    data = GreEx.fetch(:data, cloud_event)
    assert data.message == "Test message"
    assert data.value == 42
  end
  
  test "passes through existing CloudEvents" do
    # Create a CloudEvent
    cloud_event = GreEx.make([
      specversion: "1.0",
      id: "123e4567-e89b-12d3-a456-426614174000",
      source: "original-source",
      type: "io.original.event",
      time: "2023-01-01T12:00:00Z",
      data: %{message: "Original message"}
    ])
    
    # Process through the plugin
    {:ok, processed_event} = GoldrushEx.Plugin.process_event(cloud_event)
    
    # Verify it's unchanged
    assert GreEx.fetch(:source, processed_event) == "original-source"
    assert GreEx.fetch(:type, processed_event) == "io.original.event"
    assert GreEx.fetch(:data, processed_event).message == "Original message"
  end
end
