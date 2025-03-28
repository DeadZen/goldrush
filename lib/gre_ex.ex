defmodule GreEx do
  @moduledoc """
  Elixir wrapper for the Goldrush event functions.
  
  This module provides functions for creating and manipulating events.
  """

  @doc """
  Creates a new event.
  
  ## Parameters
  
  - `pairs`: A list of key-value pairs
  - `opts`: Options for the event (default: [list])
  
  ## Returns
  
  A new event
  """
  def make(pairs, opts \\ [:list]) do
    :gre.make(pairs, opts)
  end

  @doc """
  Checks if a field exists in an event.
  
  ## Parameters
  
  - `key`: The field name
  - `event`: The event to check
  
  ## Returns
  
  `true` if the field exists, `false` otherwise
  """
  def has(key, event) do
    :gre.has(key, event)
  end

  @doc """
  Gets the value of a field in an event.
  
  ## Parameters
  
  - `key`: The field name
  - `event`: The event to check
  
  ## Returns
  
  The value of the field
  
  ## Raises
  
  `ArgumentError` if the field does not exist
  """
  def fetch(key, event) do
    :gre.fetch(key, event)
  end

  @doc """
  Finds the value of a field in an event.
  
  ## Parameters
  
  - `key`: The field name
  - `event`: The event to check
  
  ## Returns
  
  `{true, value}` if the field exists, `false` otherwise
  """
  def find(key, event) do
    :gre.find(key, event)
  end

  @doc """
  Gets the names of all fields in an event.
  
  ## Parameters
  
  - `event`: The event to check
  
  ## Returns
  
  A list of field names
  """
  def keys(event) do
    :gre.keys(event)
  end

  @doc """
  Gets the name and value of all fields in an event.
  
  ## Parameters
  
  - `event`: The event to check
  
  ## Returns
  
  A list of `{key, value}` tuples
  """
  def pairs(event) do
    :gre.pairs(event)
  end

  @doc """
  Appends a key-value pair to an event.
  
  ## Parameters
  
  - `key_val`: The key-value pair to append
  - `event`: The event to append to
  
  ## Returns
  
  The updated event
  """
  def append(key_val, event) do
    :gre.append(key_val, event)
  end

  @doc """
  Merges two events.
  
  ## Parameters
  
  - `event1`: The first event
  - `event2`: The second event
  
  ## Returns
  
  The merged event
  """
  def merge(event1, event2) do
    :gre.merge(event1, event2)
  end
end
