defmodule RemotePersistentTerm.Fetcher.Http do
  @moduledoc """
  A Fetcher implementation for HTTP.
  """
  require Logger
  alias RemotePersistentTerm.Fetcher.Http.Cache

  @behaviour RemotePersistentTerm.Fetcher

  @type t :: %__MODULE__{
          url: String.t(),
          http_cache?: boolean()
        }

  defstruct [:url, :http_cache?]

  @opts_schema [
    url: [
      type: :string,
      required: true,
      doc: "The url from which the remote term is downloaded."
    ],
    http_cache?: [
      type: :boolean,
      default: false,
      doc: """
      If true, the HTTP Caching spec will be used to schedule the next download.

      Should avoid setting both this value to true and `refresh_interval` to a value.
      """
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
         url: valid_opts[:url],
         http_cache?: valid_opts[:http_cache?]
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

    with {:ok, resp} <- Req.get(state.url),
         :ok <- response_status(state.url, resp.status),
         :ok <- schedule(resp, state.http_cache?) do
      {:ok, resp.body}
    end
  end

  defp response_status(url, status) do
    if status < 300 do
      Logger.info("successfully downloaded remote term from #{url} with status #{status}")
      :ok
    else
      {:error, {:http_status, status}}
    end
  end

  defp schedule(resp, true) do
    with {:ok, refresh_interval} <- Cache.refresh_interval(resp) do
      RemotePersistentTerm.schedule_update(self(), refresh_interval)
      :ok
    end
  end

  defp schedule(_resp, _state), do: :ok
end
