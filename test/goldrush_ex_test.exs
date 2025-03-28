defmodule GoldrushExTest do
  use ExUnit.Case
  doctest GoldrushEx

  setup do
    GoldrushEx.start()
    :ok
  end

  test "compiles a simple query" do
    query = GoldrushEx.eq(:test_field, "test_value")
    assert {:ok, :test_handler} = GoldrushEx.compile(:test_handler, query)
    
    # Clean up
    GoldrushEx.delete(:test_handler)
  end

  test "handles an event" do
    query = GoldrushEx.with(
      GoldrushEx.eq(:test_field, "test_value"),
      fn _event -> :ok end
    )
    
    {:ok, :test_handler} = GoldrushEx.compile(:test_handler, query)
    
    # Handle an event
    GoldrushEx.handle(:test_handler, [test_field: "test_value"])
    
    # Check counters
    assert GoldrushEx.input(:test_handler) == 1
    assert GoldrushEx.output(:test_handler) == 1
    assert GoldrushEx.filter(:test_handler) == 0
    
    # Clean up
    GoldrushEx.delete(:test_handler)
  end

  test "filters events" do
    query = GoldrushEx.with(
      GoldrushEx.eq(:test_field, "test_value"),
      fn _event -> :ok end
    )
    
    {:ok, :test_handler} = GoldrushEx.compile(:test_handler, query)
    
    # Handle matching event
    GoldrushEx.handle(:test_handler, [test_field: "test_value"])
    
    # Handle non-matching event
    GoldrushEx.handle(:test_handler, [test_field: "other_value"])
    
    # Check counters
    assert GoldrushEx.input(:test_handler) == 2
    assert GoldrushEx.output(:test_handler) == 1
    assert GoldrushEx.filter(:test_handler) == 1
    
    # Clean up
    GoldrushEx.delete(:test_handler)
  end

  test "runs a job" do
    query = GoldrushEx.null(true)
    
    {:ok, :test_handler} = GoldrushEx.compile(:test_handler, query)
    
    # Run a job
    {time, result} = GoldrushEx.run(:test_handler, fn _event, _store ->
      "job_result"
    end, [test_field: "test_value"])
    
    # Check result
    assert is_number(time)
    assert result == "job_result"
    assert GoldrushEx.job_input(:test_handler) == 1
    assert GoldrushEx.job_run(:test_handler) == 1
    
    # Clean up
    GoldrushEx.delete(:test_handler)
  end

  test "resets counters" do
    query = GoldrushEx.null(true)
    
    {:ok, :test_handler} = GoldrushEx.compile(:test_handler, query)
    
    # Handle some events
    GoldrushEx.handle(:test_handler, [test_field: "test_value"])
    GoldrushEx.handle(:test_handler, [test_field: "other_value"])
    
    # Check counters
    assert GoldrushEx.input(:test_handler) == 2
    
    # Reset counters
    GoldrushEx.reset_counters(:test_handler)
    
    # Check counters again
    assert GoldrushEx.input(:test_handler) == 0
    
    # Clean up
    GoldrushEx.delete(:test_handler)
  end

  test "stores and retrieves values" do
    query = GoldrushEx.null(true)
    store = [config: [threshold: 10]]
    
    {:ok, :test_handler} = GoldrushEx.compile(:test_handler, query, store)
    
    # Get stored value
    assert {:ok, [threshold: 10]} = GoldrushEx.get(:test_handler, :config)
    
    # Clean up
    GoldrushEx.delete(:test_handler)
  end
end
