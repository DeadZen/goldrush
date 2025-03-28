defmodule GoldrushEx do
  @moduledoc """
  Elixir wrapper for the Goldrush Erlang library.
  
  Goldrush is a fast event stream processor that allows you to filter, transform,
  and react to events in real-time.
  """

  @doc """
  Starts the Goldrush application.
  """
  def start do
    _ = Application.start(:goldrush) # likely already started
    _ = GoldrushEx.Plugin.start()
  end

  @doc """
  Compiles a query to a module.
  
  ## Parameters
  
  - `module`: The name of the module to create (as an atom)
  - `query`: The query to compile
  - `store`: Optional storage for the query (default: [])
  - `reset`: Whether to reset counters (default: true)
  
  ## Returns
  
  `{:ok, module}` on success
  """
  def compile(module, query, store \\ [], reset \\ true) do
    :glc.compile(module, query, store, reset)
  end

  @doc """
  Handles an event using a compiled query.
  
  ## Parameters
  
  - `module`: The compiled query module
  - `event`: The event to process (list of key-value pairs or a proper event)
  
  ## Returns
  
  `:ok`
  """
  def handle(module, event) do
    # Process through plugins if available
    processed_event = if Code.ensure_loaded?(GoldrushEx.Plugin) do
      case GoldrushEx.Plugin.process_event(event) do
        {:ok, new_event} -> new_event
        {:skip, _reason} -> event  # Use original event if skipped
      end
    else
      event
    end
    
    :glc.handle(module, processed_event)
  end

  @doc """
  Runs a function through a compiled query module.
  
  ## Parameters
  
  - `module`: The compiled query module
  - `fun`: The function to run
  - `event`: The event to process
  
  ## Returns
  
  `{execution_time, result}` where execution_time is in microseconds
  """
  def run(module, fun, event) do
    # Process through plugins if available
    processed_event = if Code.ensure_loaded?(GoldrushEx.Plugin) do
      case GoldrushEx.Plugin.process_event(event) do
        {:ok, new_event} -> new_event
        {:skip, _reason} -> event  # Use original event if skipped
      end
    else
      event
    end
    
    :glc.run(module, fun, processed_event)
  end

  @doc """
  Gets a stored value from a compiled query module.
  
  ## Parameters
  
  - `module`: The compiled query module
  - `key`: The key to retrieve
  
  ## Returns
  
  `{:ok, value}` or `{:error, :undefined}`
  """
  def get(module, key) do
    :glc.get(module, key)
  end

  @doc """
  Deletes a compiled query module.
  
  ## Parameters
  
  - `module`: The compiled query module
  
  ## Returns
  
  `:ok`
  """
  def delete(module) do
    try do
      :glc.delete(module)
    catch
      :exit, {:noproc, _} -> :ok
      :exit, {:normal, _} -> :ok
      _, _ -> :ok
    end
  end

  @doc """
  Resets all counters in a compiled query module.
  
  ## Parameters
  
  - `module`: The compiled query module
  
  ## Returns
  
  `:ok`
  """
  def reset_counters(module) do
    :glc.reset_counters(module)
  end

  @doc """
  Resets a specific counter in a compiled query module.
  
  ## Parameters
  
  - `module`: The compiled query module
  - `counter`: The counter to reset
  
  ## Returns
  
  `:ok`
  """
  def reset_counters(module, counter) do
    :glc.reset_counters(module, counter)
  end

  @doc """
  Gets statistics about a compiled query module.
  
  ## Parameters
  
  - `module`: The compiled query module
  
  ## Returns
  
  A list of `{counter, value}` tuples
  """
  def info(module), do: :glc.info(module)
  
  @doc "Gets the number of input events for this query module."
  def input(module), do: :glc.input(module)
  
  @doc "Gets the number of output events for this query module."
  def output(module), do: :glc.output(module)
  
  @doc "Gets the number of filtered events for this query module."
  def filter(module), do: :glc.filter(module)
  
  @doc "Gets the number of job inputs for this query module."
  def job_input(module), do: :glc.job_input(module)
  
  @doc "Gets the number of job runs for this query module."
  def job_run(module), do: :glc.job_run(module)
  
  @doc "Gets the number of job errors for this query module."
  def job_error(module), do: :glc.job_error(module)
  
  @doc "Gets the amount of time jobs took for this query module."
  def job_time(module), do: :glc.job_time(module)

  # Operators
  
  @doc "Test that a field value is less than a term."
  def lt(key, term), do: :glc.lt(key, term)
  
  @doc "Test that a field value is less than or equal to a term."
  def lte(key, term), do: :glc.lte(key, term)
  
  @doc "Test that a field value is equal to a term."
  def eq(key, term), do: :glc.eq(key, term)
  
  @doc "Test that a field value is not equal to a term."
  def neq(key, term), do: :glc.neq(key, term)
  
  @doc "Test that a field value is greater than a term."
  def gt(key, term), do: :glc.gt(key, term)
  
  @doc "Test that a field value is greater than or equal to a term."
  def gte(key, term), do: :glc.gte(key, term)
  
  @doc "Test that a field exists."
  def wc(key), do: :glc.wc(key)
  
  @doc "Test that a field is not found."
  def nf(key), do: :glc.nf(key)

  # Combinators
  
  @doc "Filter the input using multiple filters (AND)."
  def all(filters), do: :glc.all(filters)
  
  @doc "Filter the input using one of multiple filters (OR)."
  def any(filters), do: :glc.any(filters)
  
  @doc "Always return `true` or `false`."
  def null(result), do: :glc.null(result)
  
  @doc "Apply a function to each output of a query."
  def with(query, action), do: :glc.with(query, action)
  
  @doc "Return a union of multiple queries."
  def union(queries), do: :glc.union(queries)
  
  @doc """
  Creates a new event from a map or keyword list.
  
  ## Examples
  
      iex> GoldrushEx.new_event(level: :info, message: "Hello")
      %GoldrushEx.Event{fields: %{level: :info, message: "Hello"}}
  """
  def new_event(fields) do
    if Code.ensure_loaded?(GoldrushEx.Event) do
      GoldrushEx.Event.new(fields)
    else
      fields
    end
  end
  
  @doc """
  Creates a new query with the given conditions.
  
  ## Examples
  
      iex> GoldrushEx.new_query(level: :error)
      {:level, :=, :error}
  """
  def new_query(conditions) do
    if Code.ensure_loaded?(GoldrushEx.Query) do
      GoldrushEx.Query.where(conditions)
    else
      case conditions do
        [{field, value}] -> eq(field, value)
        _ -> 
          conditions = Enum.map(conditions, fn {field, value} -> eq(field, value) end)
          all(conditions)
      end
    end
  end
end
