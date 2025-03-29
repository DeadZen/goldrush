defmodule Mix.Tasks.Example do
  @moduledoc """
  Runs the GoldrushEx example application.
  
  ## Examples
  
      mix example
      mix example --count 20
  """
  
  use Mix.Task
  
  @shortdoc "Runs the GoldrushEx example application"
  
  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, strict: [count: :integer])
    count = Keyword.get(opts, :count, 10)
    
    # Start required applications
    Application.ensure_all_started(:logger)
    Application.ensure_all_started(:req)
    
    # Start the example app
    Goldrush.ExampleApp.start()
    
    IO.puts("Running example with #{count} events...")
    Goldrush.ExampleApp.simulate_batch(count)
    IO.puts("Done!")
  end
end
