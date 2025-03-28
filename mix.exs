defmodule Goldrush.MixProject do
  use Mix.Project

  def project do
    [
      app: :goldrush,
      version: "0.2.1",
      licenses: ["Apache-2.0"],
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      docs: docs(),
      source_url: "https://github.com/DeadZen/goldrush"
    ]
  end

  def application do
    [
      extra_applications: [:logger, :syntax_tools],
      mod: {:gr_app, []}
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.24", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    Goldrush is a small Erlang/Elixir event processing and monitoring application.
    This is an Elixir wrapper around the Erlang Goldrush library.
    """
  end

  defp package do
    [
      files: ["lib", "src", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Elixir Contributors"],
      licenses: ["ISC"],
      links: %{"GitHub" => "https://github.com/DeadZen/goldrush"}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end
end
