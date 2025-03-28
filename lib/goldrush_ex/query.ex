defmodule GoldrushEx.Query do
  @moduledoc """
  Provides a more idiomatic way to build Goldrush queries in Elixir.
  """
  
  @type t :: term()
  
  @doc """
  Creates a new query that matches events where a field equals a value.
  
  ## Examples
  
      iex> GoldrushEx.Query.where(level: :error)
      {:level, :=, :error}
  """
  def where(clauses) when is_list(clauses) do
    case clauses do
      [{field, value}] ->
        :glc.eq(field, value)
      _ ->
        conditions = Enum.map(clauses, fn {field, value} -> :glc.eq(field, value) end)
        :glc.all(conditions)
    end
  end
  
  @doc """
  Adds a condition to a query using the given operator.
  
  ## Examples
  
      iex> query = GoldrushEx.Query.where(level: :error)
      iex> GoldrushEx.Query.where(query, :or, code: 500)
      {:level, :=, :error} OR (code = 500)>
  """
  def where(query, :and, clauses) when is_list(clauses) do
    new_conditions = Enum.map(clauses, fn {field, value} -> :glc.eq(field, value) end)
    :glc.all([query | new_conditions])
  end
  
  def where(query, :or, clauses) when is_list(clauses) do
    new_conditions = Enum.map(clauses, fn {field, value} -> :glc.eq(field, value) end)
    :glc.any([query | new_conditions])
  end
  
  @doc """
  Adds a greater than condition to a query.
  
  ## Examples
  
      iex> GoldrushEx.Query.where(level: :error) |> GoldrushEx.Query.gt(:code, 400)
      {:level, :=, :error} AND (code > 400)>
  """
  def gt(query, field, value) do
    :glc.all([query, :glc.gt(field, value)])
  end
  
  @doc """
  Adds a less than condition to a query.
  
  ## Examples
  
      iex> GoldrushEx.Query.where(level: :error) |> GoldrushEx.Query.lt(:code, 600)
      {:level, :=, :error} AND (code < 600)>
  """
  def lt(query, field, value) do
    :glc.all([query, :glc.lt(field, value)])
  end
  
  @doc """
  Adds an action to be executed when the query matches.
  
  ## Examples
  
      iex> query = GoldrushEx.Query.where(level: :error)
      iex> GoldrushEx.Query.action(query, fn event_data -> IO.puts("Error: \#{event_data.message}") end)
      {:level, :=, :error} -> action>
  """
  def action(query, fun) when is_function(fun, 1) do
    :glc.with(query, fn internal_event ->
      event = GoldrushEx.Event.from_internal(internal_event)
      fun.(event)
    end)
  end
  
  def action(query, fun) when is_function(fun, 2) do
    :glc.with(query, fun)
  end
end
