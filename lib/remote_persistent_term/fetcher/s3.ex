defmodule RemotePersistentTerm.Fetcher.S3 do
  @moduledoc """
  A Fetcher implementation for AWS S3.
  """
  require Logger

  @behaviour RemotePersistentTerm.Fetcher

  @type t :: %__MODULE__{
          client: AWS.Client.t(),
          bucket: String.t(),
          key: String.t()
        }
  defstruct [:client, :bucket, :key]

  @opts_schema [
    bucket: [
      type: :string,
      required: true,
      doc: "The s3 bucket in which the remote term is stored."
    ],
    key: [
      type: :string,
      required: true,
      doc: "The key within the s3 bucket which refers to the remote term."
    ]
  ]
  @doc """
  Initialize an S3 Fetcher.

  Requires AWS credentials to be provided via env vars.
  The following vars must be set to the appropriate values:
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `AWS_DEFAULT_REGION`

  Options:
  #{NimbleOptions.docs(@opts_schema)}
  """
  @impl true
  def init(opts) do
    with {:ok, valid_opts} <- NimbleOptions.validate(opts, @opts_schema) do
      client =
        AWS.Client.create()
        |> AWS.Client.put_http_client({AWS.HTTPClient.Finch, []})

      {:ok,
       %__MODULE__{
         client: client,
         bucket: valid_opts[:bucket],
         key: valid_opts[:key]
       }}
    end
  end

  @impl true
  def current_version(state) do
    with {:ok, %{"ListBucketResult" => %{"Contents" => contents}}, _} <-
           AWS.S3.list_objects_v2(state.client, state.bucket),
         %{e_tag: etag} when is_binary(etag) <- find_latest(contents, state.key) do
      Logger.info("found latest version of s3://#{state.bucket}/#{state.key}: #{etag}")
      {:ok, etag}
    else
      {:error, {:http_error, _status, %{body: error}}} ->
        {:error, error}

      _ ->
        {:error, "could not find s3://#{state.bucket}/#{state.key}"}
    end
  end

  @impl true
  def download(state) do
    Logger.info("downloading s3://#{state.bucket}/#{state.key}...")

    case AWS.S3.get_object(state.client, state.bucket, state.key) do
      {:ok, _, %{body: body}} ->
        Logger.debug("downloaded s3://#{state.bucket}/#{state.key}!")
        {:ok, body}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp find_latest(contents, key) do
    Enum.find(contents, fn
      %{key: ^key} -> true
      _ -> false
    end)
  end
end
