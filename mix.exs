defmodule RemotePersistentTerm.MixProject do
  use Mix.Project

  def project do
    [
      app: :remote_persistent_term,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:nimble_options, "~> 1.0"},
      {:aws, "~> 0.13.0"},
      {:finch, "~> 0.16"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false}
    ]
  end
end
