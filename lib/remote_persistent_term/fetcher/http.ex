defmodule RemotePersistentTerm.Fetcher.Http do
  @moduledoc """
  A Fetcher implementation for HTTP.
  """
  require Logger
  alias RemotePersistentTerm.Fetcher.Http.Cache

  @behaviour RemotePersistentTerm.Fetcher

  @type t :: %__MODULE__{
          url: String.t(),
          http_cache?: boolean(),
          min_refresh_interval_ms: pos_integer()
        }

  defstruct [:url, :http_cache?, :min_refresh_interval_ms]

  @opts_schema [
    url: [
      type: :string,
      required: true,
      doc: "The url from which the remote term is downloaded."
    ],
    http_cache: [
      type: :keyword_list,
      doc: "Configuration options for the HTTP Caching spec.",
      default: [],
      keys: [
        enabled?: [
          type: :boolean,
          default: false,
          doc: """
          If true, the HTTP Caching spec will be used to schedule the next download and `:refresh_interval` can be omitted. 
          """
        ],
        min_refresh_interval_ms: [
          type: :non_neg_integer,
          default: :timer.seconds(30),
          doc: """
          The minimum time in milliseconds between refreshes.
          This value is only used if `http_cache.enabled?` is true.
          """
        ]
      ]
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
      http_cache = Keyword.get(valid_opts, :http_cache, [])

      {:ok,
       %__MODULE__{
         url: valid_opts[:url],
         http_cache?: http_cache[:enabled?],
         min_refresh_interval_ms: http_cache[:min_refresh_interval_ms]
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

    with {:ok, resp} <- Req.get(state.url, cache: false),
         :ok <- response_status(state.url, resp.status),
         :ok <- schedule(resp, state) do
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

  defp schedule(resp, %{http_cache?: true} = state) do
    with {:ok, refresh_interval} <- Cache.refresh_interval(resp) do
      refresh_interval = max(refresh_interval, state.min_refresh_interval_ms)

      RemotePersistentTerm.schedule_update(self(), refresh_interval)
      :ok
    end
  end

  defp schedule(_resp, _state), do: :ok
end
