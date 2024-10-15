defmodule DuckCaller.MixProject do
  use Mix.Project

  def project do
    [
      app: :infor_dmf,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:duckdbex, "~> 0.3.4"},
      {:xlsx_reader, "~> 0.8.7"},
      {:exceed, "~> 0.1"},
      {:table, "~> 0.1.2"},
      {:table_rex, "~> 4.0.0"},
      {:ayesql, "~> 1.1"}
      # {:credo, "~> 1.7", only: [:dev, :test]}
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
