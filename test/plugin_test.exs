defmodule GoldrushEx.PluginTest do
  use ExUnit.Case
  
  defmodule TestPlugin do
    @behaviour GoldrushEx.Plugin
    
    @impl true
    def init(opts) do
      {:ok, opts}
    end
    
    @impl true
    def handle_event(event, _state) do
      # Add a marker to the event
      event_with_marker = GreEx.make(
        [{:processed_by, __MODULE__} | GreEx.pairs(event)]
      )
      
      {:ok, event_with_marker}
    end
    
    @impl true
    def terminate(_reason, _state) do
      :ok
    end
  end
  
  setup do
    # Ensure Goldrush and the plugin system are started
    GoldrushEx.start()
    
    # Clean up any existing plugins
    for {id, _} <- GoldrushEx.Plugin.list() do
      GoldrushEx.Plugin.unregister(id)
    end
    
    :ok
  end
  
  test "registers and unregisters a plugin" do
    # Register a plugin
    {:ok, plugin_id} = GoldrushEx.Plugin.register(TestPlugin, [test: true])
    
    # Check that it's in the list
    plugins = GoldrushEx.Plugin.list()
    assert Enum.any?(plugins, fn {id, _} -> id == plugin_id end)
    
    # Unregister the plugin
    :ok = GoldrushEx.Plugin.unregister(plugin_id)
    
    # Check that it's no longer in the list
    plugins = GoldrushEx.Plugin.list()
    refute Enum.any?(plugins, fn {id, _} -> id == plugin_id end)
  end
  
  test "processes an event through a plugin" do
    # Register a plugin
    {:ok, plugin_id} = GoldrushEx.Plugin.register(TestPlugin, [])
    
    # Create an event
    event = GreEx.make([test_field: "test_value"])
    
    # Process the event
    {:ok, processed_event} = GoldrushEx.Plugin.process_event(event)
    
    # Check that the plugin processed it
    assert GreEx.has(:processed_by, processed_event)
    assert GreEx.fetch(:processed_by, processed_event) == TestPlugin
    
    # Clean up
    GoldrushEx.Plugin.unregister(plugin_id)
  end
  
  test "enables and disables a plugin" do
    # Register a plugin
    {:ok, plugin_id} = GoldrushEx.Plugin.register(TestPlugin, [])
    
    # Create an event
    event = GreEx.make([test_field: "test_value"])
    
    # Process the event with the plugin enabled
    {:ok, processed_event1} = GoldrushEx.Plugin.process_event(event)
    assert GreEx.has(:processed_by, processed_event1)
    
    # Disable the plugin
    :ok = GoldrushEx.Plugin.disable(plugin_id)
    
    # Process the event with the plugin disabled
    {:ok, processed_event2} = GoldrushEx.Plugin.process_event(event)
    refute GreEx.has(:processed_by, processed_event2)
    
    # Enable the plugin again
    :ok = GoldrushEx.Plugin.enable(plugin_id)
    
    # Process the event with the plugin re-enabled
    {:ok, processed_event3} = GoldrushEx.Plugin.process_event(event)
    assert GreEx.has(:processed_by, processed_event3)
    
    # Clean up
    GoldrushEx.Plugin.unregister(plugin_id)
  end
end
