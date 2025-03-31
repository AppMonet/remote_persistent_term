defmodule RemotePersistentTerm.MixProject do
  use Mix.Project

  @name "RemotePersistentTerm"
  @version "0.8.1"
  @repo_url "https://github.com/AppMonet/remote_persistent_term"

  def project do
    [
      app: :remote_persistent_term,
      version: @version,
      name: @name,
      source_url: @repo_url,
      description: "Store remote data as a persistent_term.",
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
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false},
      {:ex_aws, "~> 2.1"},
      {:ex_aws_s3, "~> 2.0"},
      {:configparser_ex, "~> 4.0", optional: true},
      {:hackney, "~> 1.9"},
      {:sweet_xml, "~> 0.6"},
      {:mox, "~> 1.0", only: :test},
      {:req, "~> 0.4"},
      {:bypass, "~> 2.1", only: :test}
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
