# Start Goldrush before running tests
Application.ensure_all_started(:goldrush)
GoldrushEx.start()
ExUnit.start()
