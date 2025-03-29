defmodule GoldrushEx.RouterTest do
  use ExUnit.Case
  import ExUnit.CaptureLog
  
  setup do
    # Ensure Goldrush is started
    Application.ensure_all_started(:goldrush)
    
    # Create test handlers
    setup_test_handlers()
    
    # Register the Router plugin
    {:ok, plugin_id} = GoldrushEx.Plugin.register(GoldrushEx.Plugins.Router, [
      routes: [
        {{:level, :eq, :error}, :test_error_handler},
        {{:level, :eq, :warning}, :test_warning_handler},
        {{:source, :eq, :test}, :test_source_handler}
      ],
      default_handler: :test_default_handler
    ])
    
    on_exit(fn ->
      # Clean up
      GoldrushEx.Plugin.unregister(plugin_id)
      GoldrushEx.delete(:test_error_handler)
      GoldrushEx.delete(:test_warning_handler)
      GoldrushEx.delete(:test_source_handler)
      GoldrushEx.delete(:test_default_handler)
    end)
    
    :ok
  end
  
  test "routes events based on level" do
    # Create an error event
    error_event = GreEx.make([
      level: :error,
      message: "Error message"
    ])

    # Process and check routing
    log = capture_log(fn ->
      {:ok, _} = GoldrushEx.Plugin.process_event(error_event)
    end)
    
    assert log =~ "Error handler received"
    assert log =~ "Error message"
    
    # Create a warning event
    warning_event = GreEx.make([
      level: :warning,
      message: "Warning message"
    ])
    
    # Process and check routing
    log = capture_log(fn ->
      {:ok, _} = GoldrushEx.Plugin.process_event(warning_event)
    end)
    
    assert log =~ "Warning handler received"
    assert log =~ "Warning message"
  end
  
  test "routes events based on source" do
    # Create a test source event
    test_event = GreEx.make([
      source: :test,
      message: "Test message"
    ])
    
    # Process and check routing
    log = capture_log(fn ->
      {:ok, _} = GoldrushEx.Plugin.process_event(test_event)
    end)
    
    assert log =~ "Test handler received"
    assert log =~ "Test message"
  end
  
  test "uses default handler for non-matching events" do
    # Create a non-matching event
    event = GreEx.make([
      level: :info,
      source: :other,
      message: "Info message"
    ])
    
    # Process and check routing
    log = capture_log(fn ->
      {:ok, _} = GoldrushEx.Plugin.process_event(event)
    end)
    
    assert log =~ "Default handler received"
    assert log =~ "Info message"
  end
  
  defp setup_test_handlers do
    # Error handler
    error_query = GoldrushEx.with(
      GoldrushEx.eq(:level, :error),
      fn event ->
        require Logger
        Logger.error("Error handler received: #{GreEx.get(:message, event)}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:test_error_handler, error_query)
    
    # Warning handler
    warning_query = GoldrushEx.with(
      GoldrushEx.eq(:level, :warning),
      fn event ->
        require Logger
        Logger.warning("Warning handler received: #{GreEx.get(:message, event)}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:test_warning_handler, warning_query)
    
    # Test handler
    test_query = GoldrushEx.with(
      GoldrushEx.eq(:source, :test),
      fn event ->
        require Logger
        Logger.info("Test handler received: #{GreEx.get(:message, event)}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:test_source_handler, test_query)
    
    # Default handler
    default_query = GoldrushEx.with(
      GoldrushEx.null(true),
      fn event ->
        require Logger
        Logger.info("Default handler received: #{GreEx.get(:message, event)}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:test_default_handler, default_query)
  end
end
