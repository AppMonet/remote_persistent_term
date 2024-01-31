defmodule RemotePersistentTerm.Fetcher.Http do
  @moduledoc """
  A Fetcher implementation for HTTP.
  """
  require Logger

  @behaviour RemotePersistentTerm.Fetcher

  @type t :: %__MODULE__{
          url: String.t()
        }

  defstruct [:url]

  @opts_schema [
    url: [
      type: :string,
      required: true,
      doc: "The url from which the remote term is downloaded."
    ]
  ]

  @doc """
  Initialize a HTTP Fetcher.

  Options:
  #{NimbleOptions.docs(@opts_schema)}
  """
  @impl true
  def init(opts) do
    with {:ok, valid_opts} <- NimbleOptions.validate(opts, @opts_schema) do
      {:ok,
       %__MODULE__{
         url: valid_opts[:url]
       }}
    end
  end

  @impl true
  def current_version(_state) do
    {:ok, DateTime.utc_now() |> DateTime.to_string()}
  end

  @impl true
  def download(state) do
    Logger.info("downloading remote term from #{state.url}")

    with {:ok, resp} <- Req.get(state.url, cache: true) do
      if resp.status < 300 do
        Logger.info(
          "successfully downloaded remote term from #{state.url} with status #{resp.status}"
        )

        {:ok, resp.body}
      else
        {:error, {:status, resp.status}}
      end
    else
      {:error, reason} ->
        {:error, reason}
    end
  end
end
