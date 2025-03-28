defmodule GoldrushEx.Plugin do
  @moduledoc """
  Defines a behavior for Goldrush plugins and provides the plugin registry.
  
  Plugins can extend Goldrush's functionality in various ways:
  - Adding new query operators
  - Providing custom event processors
  - Adding integration with other systems
  - Extending the statistics and monitoring capabilities
  """
  
  @callback init(opts :: keyword()) :: {:ok, state :: term()} | {:error, reason :: term()}
  @callback handle_event(event :: GoldrushEx.Event.t(), state :: term()) :: 
    {:ok, event :: GoldrushEx.Event.t()} | {:skip, reason :: term()}
  @callback terminate(reason :: term(), state :: term()) :: term()
  
  @optional_callbacks terminate: 2
  
  # Plugin registry
  @registry_name :goldrush_plugin_registry
  
  @doc """
  Starts the plugin system.
  """
  def start do
    # Create the plugin registry
    case Registry.start_link(keys: :unique, name: @registry_name) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      error -> error
    end
    
    # Load plugins from application config
    load_configured_plugins()
    
    :ok
  end
  
  @doc """
  Registers a plugin with the system.
  
  ## Parameters
  
  - `plugin_module`: The module implementing the GoldrushEx.Plugin behavior
  - `opts`: Options to pass to the plugin's init/1 callback
  
  ## Returns
  
  - `{:ok, plugin_id}` on success
  - `{:error, reason}` on failure
  """
  def register(plugin_module, opts \\ []) do
    plugin_id = generate_plugin_id(plugin_module)
    
    # Initialize the plugin
    case plugin_module.init(opts) do
      {:ok, state} ->
        # Register the plugin
        case Registry.register(@registry_name, plugin_id, %{
          module: plugin_module,
          state: state,
          enabled: true
        }) do
          {:ok, _} -> {:ok, plugin_id}
          {:error, reason} -> {:error, reason}
        end
      
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  @doc """
  Unregisters a plugin from the system.
  
  ## Parameters
  
  - `plugin_id`: The ID of the plugin to unregister
  
  ## Returns
  
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def unregister(plugin_id) do
    # Get the plugin data
    case lookup_plugin(plugin_id) do
      {:ok, %{module: module, state: state}} ->
        # Call terminate if implemented
        if function_exported?(module, :terminate, 2) do
          module.terminate(:normal, state)
        end
        
        # Unregister from registry
        Registry.unregister(@registry_name, plugin_id)
        :ok
      
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  @doc """
  Enables a plugin.
  
  ## Parameters
  
  - `plugin_id`: The ID of the plugin to enable
  
  ## Returns
  
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def enable(plugin_id) do
    update_plugin_status(plugin_id, true)
  end
  
  @doc """
  Disables a plugin.
  
  ## Parameters
  
  - `plugin_id`: The ID of the plugin to disable
  
  ## Returns
  
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  def disable(plugin_id) do
    update_plugin_status(plugin_id, false)
  end
  
  @doc """
  Lists all registered plugins.
  
  ## Returns
  
  A list of `{plugin_id, plugin_data}` tuples.
  """
  def list do
    Registry.select(@registry_name, [{{:"$1", :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
  end
  
  @doc """
  Processes an event through all enabled plugins.
  
  ## Parameters
  
  - `event`: The event to process
  
  ## Returns
  
  - `{:ok, event}` if the event was processed successfully
  - `{:skip, reason}` if the event should be skipped
  """
  def process_event(event) do
    # Get all enabled plugins
    plugins = Enum.filter(list(), fn {_id, data} -> data.enabled end)
    
    # Process the event through each plugin
    Enum.reduce_while(plugins, {:ok, event}, fn {_id, %{module: module, state: state}}, {:ok, acc_event} ->
      case module.handle_event(acc_event, state) do
        {:ok, new_event} -> {:cont, {:ok, new_event}}
        {:skip, reason} -> {:halt, {:skip, reason}}
      end
    end)
  end
  
  # Private functions
  
  defp generate_plugin_id(module) do
    "#{module}_#{:erlang.unique_integer([:positive])}"
  end
  
  defp lookup_plugin(plugin_id) do
    case Registry.lookup(@registry_name, plugin_id) do
      [{_pid, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end
  
  defp update_plugin_status(plugin_id, enabled) do
    case lookup_plugin(plugin_id) do
      {:ok, data} ->
        Registry.update_value(@registry_name, plugin_id, fn _ ->
          %{data | enabled: enabled}
        end)
        :ok
      
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  defp load_configured_plugins do
    plugins = Application.get_env(:goldrush, :plugins, [])
    
    for {plugin_module, opts} <- plugins do
      register(plugin_module, opts)
    end
  end
end
