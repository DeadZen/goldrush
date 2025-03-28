defmodule GoldrushEx.JobSystemTest do
  use ExUnit.Case

  @moduledoc """
  Tests for the Goldrush job system using a data processing pipeline example.
  """

  defmodule DataProcessor do
    @moduledoc """
    Example application that processes data using Goldrush jobs.
    """
    
    def start do
      # Ensure Goldrush is started
      Application.ensure_all_started(:goldrush)
      
      # Delete any existing processor to start fresh
      try do
        GoldrushEx.delete(:data_processor)
      catch
        _, _ -> :ok
      end
      
      # Define a query that matches events with data to process
      query = GoldrushEx.with(
        GoldrushEx.wc(:data),
        fn _event -> :ok end
      )
      
      # Compile the query
      {:ok, :data_processor} = GoldrushEx.compile(:data_processor, query, [
        pipeline_config: [
          batch_size: 100,
          timeout: 5000,
          retry_count: 3
        ],
        statistics: true
      ])
      
      :ok
    end
    
    def process_data(data, options \\ []) do
      # Create an event with the data
      event = GreEx.make([
        data: data,
        priority: Keyword.get(options, :priority, :normal),
        timestamp: DateTime.utc_now()
      ])
      
      # Run the processing job
      GoldrushEx.run(:data_processor, &process_job/2, event)
    end
    
    def process_batch(data_list, options \\ []) do
      # Process multiple items in a batch
      Enum.map(data_list, fn data ->
        process_data(data, options)
      end)
    end
    
    def get_stats do
      %{
        processed: GoldrushEx.job_run(:data_processor),
        errors: GoldrushEx.job_error(:data_processor),
        avg_time_ms: avg_processing_time()
      }
    end
    
    defp avg_processing_time do
      job_time = GoldrushEx.job_time(:data_processor)
      job_count = GoldrushEx.job_run(:data_processor)
      
      if job_count > 0 do
        job_time / job_count / 1000
      else
        0
      end
    end
    
    defp process_job(event, store) do
      # Extract data and configuration
      data = GreEx.fetch(:data, event)
      config = Keyword.get(store, :pipeline_config, [])
      
      # Simulate processing based on data type
      case data do
        %{type: :text, content: content} ->
          # Text processing
          processed_text = String.upcase(content)
          %{type: :processed_text, result: processed_text}
          
        %{type: :image, content: content} ->
          # Image processing (simulated)
          Process.sleep(50) # Simulate processing time
          %{type: :processed_image, result: "Processed #{byte_size(content)} bytes"}
          
        %{type: :error_case} ->
          # Simulate an error
          raise "Processing error: #{inspect(data)}"
          
        %{type: :timeout_case} ->
          # Simulate a timeout
          timeout = Keyword.get(config, :timeout, 5000)
          Process.sleep(timeout + 100)
          %{type: :timeout_result, result: "This should time out"}
          
        _ ->
          # Default processing
          %{type: :processed_unknown, result: "Processed unknown data"}
      end
    end
  end
  
  setup do
    # Ensure Goldrush is started
    Application.ensure_all_started(:goldrush)
    
    # Start the data processor
    DataProcessor.start()
    
    # Reset counters before each test
    GoldrushEx.reset_counters(:data_processor)
    
    on_exit(fn ->
      # Clean up safely
      try do
        GoldrushEx.delete(:data_processor)
      catch
        _, _ -> :ok
      end
    end)
    
    :ok
  end
  
  test "processes text data successfully" do
    text_data = %{type: :text, content: "hello world"}
    
    {time, result} = DataProcessor.process_data(text_data)
    
    # Check result
    assert is_number(time)
    assert result.type == :processed_text
    assert result.result == "HELLO WORLD"
    
    # Check stats
    stats = DataProcessor.get_stats()
    assert stats.processed == 1
    assert stats.errors == 0
    assert stats.avg_time_ms > 0
  end
  
  test "processes image data successfully" do
    image_data = %{type: :image, content: String.duplicate("pixel", 1000)}
    
    {time, result} = DataProcessor.process_data(image_data)
    
    # Check result
    assert is_number(time)
    assert result.type == :processed_image
    assert result.result =~ "Processed 5000 bytes"
    
    # Check stats
    stats = DataProcessor.get_stats()
    assert stats.processed == 1
    assert stats.errors == 0
    assert stats.avg_time_ms >= 50 # Should take at least 50ms
  end
  
  test "handles processing errors" do
    error_data = %{type: :error_case}
    
    {time, result} = DataProcessor.process_data(error_data)
    
    # Check result
    assert is_number(time)
    assert match?({:error, _}, result)
    
    # Check stats
    stats = DataProcessor.get_stats()
    assert stats.processed == 0
    assert stats.errors == 1
  end
  
  test "processes batches of data" do
    batch_data = [
      %{type: :text, content: "first item"},
      %{type: :text, content: "second item"},
      %{type: :text, content: "third item"}
    ]
    
    results = DataProcessor.process_batch(batch_data)
    
    # Check results
    assert length(results) == 3
    
    for {{time, result}, index} <- Enum.with_index(results) do
      assert is_number(time)
      assert result.type == :processed_text
      assert result.result == String.upcase(Enum.at(batch_data, index).content)
    end
    
    # Check stats
    stats = DataProcessor.get_stats()
    assert stats.processed == 3
    assert stats.errors == 0
  end
  
  test "handles mixed success and failure in batches" do
    mixed_batch = [
      %{type: :text, content: "good item"},
      %{type: :error_case},
      %{type: :text, content: "another good item"}
    ]
    
    results = DataProcessor.process_batch(mixed_batch)
    
    # Check results
    assert length(results) == 3
    
    # First item should succeed
    {_, first_result} = Enum.at(results, 0)
    assert first_result.type == :processed_text
    assert first_result.result == "GOOD ITEM"
    
    # Second item should fail
    {_, second_result} = Enum.at(results, 1)
    assert match?({:error, _}, second_result)
    
    # Third item should succeed
    {_, third_result} = Enum.at(results, 2)
    assert third_result.type == :processed_text
    assert third_result.result == "ANOTHER GOOD ITEM"
    
    # Check stats
    stats = DataProcessor.get_stats()
    assert stats.processed == 2
    assert stats.errors == 1
  end
  
  test "captures and reports job execution time" do
    # Process an item that takes a known amount of time
    image_data = %{type: :image, content: String.duplicate("pixel", 1000)}
    
    {time, _} = DataProcessor.process_data(image_data)
    
    # Time should be at least 50ms (in microseconds)
    assert time >= 50_000
    
    # Check that the time is recorded in stats
    assert GoldrushEx.job_time(:data_processor) >= 50_000
  end
end
