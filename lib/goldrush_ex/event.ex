defmodule GoldrushEx.Event do
  @moduledoc """
  Represents an event in the GoldrushEx system.
  
  This struct provides a more idiomatic way to work with events in Elixir.
  """
  
  @type t :: %__MODULE__{
    fields: map(),
    metadata: map()
  }
  
  defstruct fields: %{}, metadata: %{}
  
  @doc """
  Creates a new event from a map or keyword list.
  
  ## Examples
  
      iex> GoldrushEx.Event.new(level: :info, message: "Hello")
      %GoldrushEx.Event{fields: %{level: :info, message: "Hello"}}
      
      iex> GoldrushEx.Event.new(%{level: :error, code: 500})
      %GoldrushEx.Event{fields: %{level: :error, code: 500}}
  """
  def new(fields) when is_list(fields) do
    new(Map.new(fields))
  end
  
  def new(fields) when is_map(fields) do
    %__MODULE__{fields: fields}
  end
  
  @doc """
  Adds metadata to an event.
  
  ## Examples
  
      iex> event = GoldrushEx.Event.new(message: "Hello")
      iex> GoldrushEx.Event.put_metadata(event, source: "API")
      %GoldrushEx.Event{fields: %{message: "Hello"}, metadata: %{source: "API"}}
  """
  def put_metadata(%__MODULE__{} = event, metadata) when is_list(metadata) do
    put_metadata(event, Map.new(metadata))
  end
  
  def put_metadata(%__MODULE__{metadata: current} = event, metadata) when is_map(metadata) do
    %{event | metadata: Map.merge(current, metadata)}
  end
  
  @doc """
  Gets a field from the event.
  
  ## Examples
  
      iex> event = GoldrushEx.Event.new(message: "Hello")
      iex> GoldrushEx.Event.get(event, :message)
      "Hello"
      
      iex> GoldrushEx.Event.get(event, :missing, :default)
      :default
  """
  def get(%__MODULE__{fields: fields}, key, default \\ nil) do
    Map.get(fields, key, default)
  end
  
  @doc """
  Puts a field in the event.
  
  ## Examples
  
      iex> event = GoldrushEx.Event.new(message: "Hello")
      iex> GoldrushEx.Event.put(event, :level, :info)
      %GoldrushEx.Event{fields: %{message: "Hello", level: :info}}
  """
  def put(%__MODULE__{fields: fields} = event, key, value) do
    %{event | fields: Map.put(fields, key, value)}
  end
  
  @doc """
  Converts the event to the internal Goldrush format.
  """
  def to_internal(%__MODULE__{fields: fields}) do
    fields_list = Enum.into(fields, [])
    :gre.make(fields_list, [:list])
  end
  
  @doc """
  Creates an event from the internal Goldrush format.
  """
  def from_internal({:list, fields}) do
    fields_map = Map.new(fields)
    %__MODULE__{fields: fields_map}
  end
end
