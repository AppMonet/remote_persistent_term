defmodule RemotePersistentTerm.Fetcher.S3 do
  @moduledoc """
  A Fetcher implementation for AWS S3.
  """
  require Logger

  @behaviour RemotePersistentTerm.Fetcher
  @aws_client Application.compile_env!(:remote_persistent_term, :aws_client)

  @type t :: %__MODULE__{
          bucket: String.t(),
          key: String.t()
        }
  defstruct [:bucket, :key]

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
      {:ok,
       %__MODULE__{
         bucket: valid_opts[:bucket],
         key: valid_opts[:key]
       }}
    end
  end

  @impl true
  def current_version(state) do
    with {:ok, %{body: %{contents: contents}}} <-
           ExAws.S3.list_objects(state.bucket) |> @aws_client.request(),
         {:ok, %{e_tag: etag}} <- find_latest(contents, state.key) do
      Logger.info("found latest version of s3://#{state.bucket}/#{state.key}: #{etag}")
      {:ok, etag}
    else
      {:error, {:unexpected_response, %{body: reason}}} ->
        {:error, reason}

      {:error, :not_found} ->
        {:error, "could not find s3://#{state.bucket}/#{state.key}"}

      # Handles Mint.TransportError
      {:error, %{reason: reason}} ->
        {:error, reason}

      {:error, reason} ->
        Logger.error("#{__MODULE__} - unknown error: #{inspect(reason)}")
        {:error, "Unknown error"}
    end
  end

  @impl true
  def download(state) do
    Logger.info("downloading s3://#{state.bucket}/#{state.key}...")

    with {:ok, %{body: body}} <-
           ExAws.S3.get_object(state.bucket, state.key) |> @aws_client.request() do
      Logger.debug("downloaded s3://#{state.bucket}/#{state.key}!")
      {:ok, body}
    else
      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp find_latest([_ | _] = contents, key) do
    Enum.find(contents, fn
      %{key: ^key} ->
        true

      _ ->
        false
    end)
    |> case do
      res when is_map(res) -> {:ok, res}
      _ -> {:error, :not_found}
    end
  end

  defp find_latest(_, _), do: {:error, :not_found}
end
