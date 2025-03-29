defmodule Goldrush.ExampleApp do
  @moduledoc """
  An example application demonstrating the GoldrushEx plugin system with CloudEvents.
  
  This application shows how to use the plugin system for event processing,
  routing, and delivery in a real-world scenario.
  """
  
  require Logger
  
  @doc """
  Starts the example application.
  """
  def start do
    # Start GoldrushEx
    Application.ensure_all_started(:goldrush)
    GoldrushEx.start()
    
    # Configure telemetry
    setup_telemetry()
    
    # Register plugins
    setup_plugins()
    
    # Compile event handlers
    setup_handlers()
    
    :ok
  end
  
  @doc """
  Processes an event through the system.
  """
  def process_event(event_data) do
    # Create an event
    event = case event_data do
      %{} -> Map.to_list(event_data)
      _ -> event_data
    end
    
    # Add timestamp if not present
    event = 
      if Keyword.has_key?(event, :timestamp) do
        event
      else
        [{:timestamp, DateTime.utc_now()} | event]
      end
    
    # Process through the system
    GoldrushEx.handle(:main_handler, GreEx.make(event))
  end
  
  @doc """
  Simulates a batch of events.
  """
  def simulate_batch(count \\ 10) do
    event_types = [:user_login, :user_logout, :page_view, :button_click, :form_submit]
    
    for _ <- 1..count do
      event_type = Enum.random(event_types)
      user_id = "user_#{:rand.uniform(100)}"
      
      event = case event_type do
        :user_login ->
          [
            event_type: :user_login,
            user_id: user_id,
            ip_address: "192.168.1.#{:rand.uniform(255)}",
            user_agent: "Mozilla/5.0 (Example Browser)"
          ]
          
        :user_logout ->
          [
            event_type: :user_logout,
            user_id: user_id,
            session_duration: :rand.uniform(3600)
          ]
          
        :page_view ->
          [
            event_type: :page_view,
            user_id: user_id,
            page: "/#{Enum.random(["home", "products", "about", "contact"])}",
            referrer: Enum.random(["", "https://google.com", "https://example.com"])
          ]
          
        :button_click ->
          [
            event_type: :button_click,
            user_id: user_id,
            button_id: "btn_#{Enum.random(["submit", "cancel", "buy", "more"])}",
            page: "/#{Enum.random(["home", "products", "about", "contact"])}"
          ]
          
        :form_submit ->
          [
            event_type: :form_submit,
            user_id: user_id,
            form_id: "form_#{Enum.random(["contact", "signup", "login", "checkout"])}",
            fields_count: :rand.uniform(10)
          ]
      end

      event_data_with_timestamp = [{:timestamp, DateTime.utc_now()} | event]
      
      process_event(event_data_with_timestamp)
    end
  end
  
  # Private functions
  
  defp setup_telemetry do
    # Attach telemetry handler
    GoldrushEx.Telemetry.attach_handler(
      :example_app_handler,
      &__MODULE__.handle_telemetry_event/4
    )
  end
  
  def handle_telemetry_event([:goldrush_ex, :event, :processed], measurements, metadata, _config) do
    Logger.debug("Event processed in #{measurements.duration / 1000}ms by #{metadata.handler}")
  end
  
  def handle_telemetry_event([:goldrush_ex, :event, :filtered], measurements, metadata, _config) do
    Logger.debug("Event filtered in #{measurements.duration / 1000}ms by #{metadata.handler}")
  end
  
  def handle_telemetry_event([:goldrush_ex, :job, :executed], measurements, metadata, _config) do
    status = if metadata.status == :success, do: "successfully", else: "with error"
    Logger.debug("Job executed #{status} in #{measurements.duration / 1000}ms by #{metadata.handler}")
  end
  
  def handle_telemetry_event([:goldrush_ex, :plugin, :processed], measurements, metadata, _config) do
    Logger.debug("Plugin #{metadata.plugin} processed event in #{measurements.duration / 1000}ms")
  end
  
  defp setup_plugins do
    # Register CloudEvents plugin
    {:ok, _} = GoldrushEx.Plugin.register(GoldrushEx.Plugins.CloudEvents, [
      source: "example-app",
      type_prefix: "io.example."
    ])
    
    # Register Router plugin
    {:ok, _} = GoldrushEx.Plugin.register(GoldrushEx.Plugins.Router, [
      routes: [
        # Route user events to user_handler
        {{:event_type, :in, [:user_login, :user_logout]}, :user_handler},
        
        # Route page views to analytics_handler
        {{:event_type, :eq, :page_view}, :analytics_handler},
        
        # Route button clicks to interaction_handler
        {{:event_type, :eq, :button_click}, :interaction_handler},
        
        # Route form submissions to form_handler
        {{:event_type, :eq, :form_submit}, :form_handler}
      ],
      default_handler: :main_handler
    ])
    
    # Register Delivery plugin
    {:ok, _} = GoldrushEx.Plugin.register(GoldrushEx.Plugins.Delivery, [
      destinations: [
        # Console output for all events
        console: %{
          type: :console
        },
        
        # File output for user events
        user_log: %{
          type: :file,
          path: "user_events.log",
          filter: {:event_type, :in, [:user_login, :user_logout]}
        },
        
        # Function delivery for analytics
        analytics: %{
          type: :function,
          function: &handle_analytics_event/1,
          filter: {:event_type, :eq, :page_view}
        }
      ]
    ])
  end
  
  defp setup_handlers do
    # Main handler - catches all events
    main_query = GoldrushEx.with(
      GoldrushEx.null(true),  # Match all events
      fn event ->
        # Use EventHelper to get event_type, which handles CloudEvents properly
        event_type = GoldrushEx.EventHelper.get_field(event, :event_type)
        Logger.info("Main handler received event: #{inspect(event_type)}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:main_handler, main_query)
    
    # User handler - for user-related events
    user_query = GoldrushEx.with(
      GoldrushEx.any([
        GoldrushEx.eq(:event_type, :user_login),
        GoldrushEx.eq(:event_type, :user_logout)
      ]),
      fn event ->
        user_id = GoldrushEx.EventHelper.get_field(event, :user_id)
        event_type = GoldrushEx.EventHelper.get_field(event, :event_type)
        Logger.info("User handler: User #{user_id} #{event_type}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:user_handler, user_query)
    
    # Analytics handler - for page views
    analytics_query = GoldrushEx.with(
      GoldrushEx.eq(:event_type, :page_view),
      fn event ->
        user_id = GoldrushEx.EventHelper.get_field(event, :user_id)
        page = GoldrushEx.EventHelper.get_field(event, :page)
        Logger.info("Analytics handler: User #{user_id} viewed page #{page}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:analytics_handler, analytics_query)
    
    # Interaction handler - for button clicks
    interaction_query = GoldrushEx.with(
      GoldrushEx.eq(:event_type, :button_click),
      fn event ->
        user_id = GoldrushEx.EventHelper.get_field(event, :user_id)
        button_id = GoldrushEx.EventHelper.get_field(event, :button_id)
        Logger.info("Interaction handler: User #{user_id} clicked button #{button_id}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:interaction_handler, interaction_query)
    
    # Form handler - for form submissions
    form_query = GoldrushEx.with(
      GoldrushEx.eq(:event_type, :form_submit),
      fn event ->
        user_id = GoldrushEx.EventHelper.get_field(event, :user_id)
        form_id = GoldrushEx.EventHelper.get_field(event, :form_id)
        Logger.info("Form handler: User #{user_id} submitted form #{form_id}")
      end
    )
    
    {:ok, _} = GoldrushEx.compile(:form_handler, form_query)
  end
  
  defp handle_analytics_event(event) do
    # In a real app, this might send to an analytics service
    user_id = GoldrushEx.EventHelper.get_field(event, :user_id)
    page = GoldrushEx.EventHelper.get_field(event, :page)
    Logger.info("Analytics event: User #{user_id} viewed page #{page}")
    {:ok, :analytics_processed}
  end
end
