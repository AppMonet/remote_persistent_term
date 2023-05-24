defmodule RemotePersistentTerm.MixProject do
  use Mix.Project

  @name "RemotePersistentTerm"
  @version "0.1.0"
  @repo_url "https://github.com/AppMonet/remote_persistent_term"

  def project do
    [
      app: :remote_persistent_term,
      version: @version,
      name: @name,
      source_url: @repo_url,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      docs: docs()
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
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @repo_url
      }
    ]
  end

  defp docs do
    [
      source_ref: "v#{@version}",
      source_url: @repo_url,
      main: @name
    ]
  end
end
