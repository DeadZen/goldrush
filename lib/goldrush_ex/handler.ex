defmodule GoldrushEx.Handler do
  @moduledoc """
  Defines a behavior for event handlers and provides macros for easy handler creation.
  """
  
  @callback handle_event(GoldrushEx.Event.t(), map()) :: any()
  @callback handle_error(term(), GoldrushEx.Event.t(), map()) :: any()
  @optional_callbacks handle_error: 3
  
  defmacro __using__(_opts) do
    quote location: :keep do
      @behaviour GoldrushEx.Handler
      
      import GoldrushEx.Query
      
      # Default implementation for handle_error
      def handle_error(error, event, _store) do
        require Logger
        Logger.error("Error handling event: #{inspect(error)}\nEvent: #{inspect(event)}")
      end
      
      # Allow overriding the default implementation
      defoverridable handle_error: 3
      
      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :worker,
          restart: :permanent,
          shutdown: 5000
        }
      end
      
      def start_link(opts \\ []) do
        GoldrushEx.Handler.start_link(__MODULE__, opts)
      end
      
      def init(opts) do
        {:ok, opts}
      end
      
      # Allow overriding the init callback
      defoverridable init: 1
    end
  end
  
  def start_link(module, opts \\ []) do
    # Initialize the handler
    {:ok, state} = module.init(opts)
    
    # Create the query based on the handler's query/0 function
    query = if function_exported?(module, :query, 0) do
      module.query()
    else
      GoldrushEx.Query.where([])
    end
    
    # Add the action to the query
    query_with_action = GoldrushEx.Query.action(query, fn event, store ->
      try do
        module.handle_event(event, store)
      rescue
        error ->
          if function_exported?(module, :handle_error, 3) do
            module.handle_error(error, event, store)
          else
            reraise error, __STACKTRACE__
          end
      end
    end)
    
    # Compile the query
    handler_name = Module.concat(module, Handler)
    {:ok, _} = GoldrushEx.compile(handler_name, query_with_action, [
      handler_module: module,
      handler_state: state,
      statistics: true
    ])
    
    # Return a fake pid (since we're not actually starting a process)
    {:ok, self()}
  end
  
  def handle_event(handler_module, event) do
    handler_name = Module.concat(handler_module, Handler)
    
    # Convert to internal format if needed
    internal_event = case event do
      %GoldrushEx.Event{} -> GoldrushEx.Event.to_internal(event)
      _ -> event
    end
    
    GoldrushEx.handle(handler_name, internal_event)
  end
end
