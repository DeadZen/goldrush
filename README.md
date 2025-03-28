# Goldrush

[![Hex.pm](https://img.shields.io/hexpm/v/goldrush.svg)](https://hex.pm/packages/goldrush)
[![Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/goldrush)

Goldrush is a small Erlang/Elixir event processing and monitoring application.

This package provides Elixir wrappers around the Erlang Goldrush library, making it easy to use from Elixir applications.

## Installation

Add `goldrush` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:goldrush, "~> 0.2.0"}
  ]
end
```

## Usage

### Basic Usage

```elixir
# Start Goldrush
GoldrushEx.start()

# Create a simple query that matches events with field 'level' equal to 'error'
query = GoldrushEx.with(
  GoldrushEx.eq(:level, :error),
  fn event ->
    # This function will be called for each matching event
    message = GreEx.fetch(:message, event)
    IO.puts("Error detected: #{message}")
  end
)

# Compile the query
{:ok, :error_handler} = GoldrushEx.compile(:error_handler, query)

# Create and process an event
event = GreEx.make([level: :error, message: "Something went wrong"])
GoldrushEx.handle(:error_handler, event)

# Get statistics
input_count = GoldrushEx.input(:error_handler)
output_count = GoldrushEx.output(:error_handler)

# Clean up
GoldrushEx.delete(:error_handler)
```

### Event Creation and Manipulation

```elixir
# Create an event
event = GreEx.make([
  user_id: "user123",
  action: "login",
  timestamp: DateTime.utc_now()
])

# Check if a field exists
if GreEx.has(:user_id, event) do
  user_id = GreEx.fetch(:user_id, event)
  IO.puts("User ID: #{user_id}")
end

# Get all field names
fields = GreEx.keys(event)

# Get all field values
pairs = GreEx.pairs(event)

# Add a field
updated_event = GreEx.append({:ip_address, "192.168.1.1"}, event)

# Merge events
event1 = GreEx.make([user_id: "user123"])
event2 = GreEx.make([session_id: "abc123"])
merged = GreEx.merge(event1, event2)
```

### Complex Queries

```elixir
# Match events where temperature is too high or too low
temperature_query = GoldrushEx.with(
  GoldrushEx.any([
    GoldrushEx.gt(:temperature, 30),
    GoldrushEx.lt(:temperature, 5)
  ]),
  fn event ->
    temp = GreEx.fetch(:temperature, event)
    IO.puts("Temperature alert: #{temp}°C")
  end
)

# Match events with multiple conditions
complex_query = GoldrushEx.with(
  GoldrushEx.all([
    GoldrushEx.eq(:level, :error),
    GoldrushEx.wc(:user_id),
    GoldrushEx.gt(:attempts, 3)
  ]),
  fn event ->
    user_id = GreEx.fetch(:user_id, event)
    attempts = GreEx.fetch(:attempts, event)
    IO.puts("Security alert: User #{user_id} failed login #{attempts} times")
  end
)
```

### Running Jobs

```elixir
# Run a job through a handler
{execution_time, result} = GoldrushEx.run(:my_handler, fn event, store ->
  # Perform some analysis or processing
  # This will be timed and statistics will be collected
  user_id = GreEx.fetch(:user_id, event)
  
  # Access stored configuration
  {:ok, config} = GoldrushEx.get(:my_handler, :config)
  threshold = Keyword.get(config, :threshold)
  
  # Return a result
  %{user_id: user_id, processed: true}
end, event)
```

## Example Applications

This package includes two example applications that demonstrate how to use Goldrush for real-time event processing:

1. **IoT Processor** - Processes and analyzes IoT sensor data in real-time
2. **Log Analyzer** - Analyzes log events in real-time

### IoT Processor Example

```elixir
# Start the IoT processor
IoTProcessor.start()

# Process an IoT event
IoTProcessor.process_event(%{
  device_id: "device123",
  temperature: 35.0,
  humidity: 60.0,
  battery: 75.0,
  timestamp: DateTime.utc_now()
})

# Get processing statistics
stats = IoTProcessor.get_stats()
```

### Log Analyzer Example

```elixir
# Start the log analyzer
LogAnalyzer.start()

# Process a log event
LogAnalyzer.process_log(%{
  level: :error,
  message: "Database connection failed",
  timestamp: DateTime.utc_now()
})

# Get processing statistics
stats = LogAnalyzer.get_stats()
```

## License

This project is licensed under the ISC License - see the LICENSE file for details.
