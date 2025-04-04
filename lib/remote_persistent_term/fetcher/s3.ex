defmodule RemotePersistentTerm.Fetcher.S3 do
  @moduledoc """
  A Fetcher implementation for AWS S3.
  """
  require Logger

  @behaviour RemotePersistentTerm.Fetcher

  @type t :: %__MODULE__{
          bucket: String.t(),
          key: String.t(),
          region: String.t()
        }
  defstruct [:bucket, :key, :region]

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
    ],
    region: [
      type: :string,
      required: true,
      doc: "The AWS region of the s3 bucket."
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
         key: valid_opts[:key],
         region: valid_opts[:region]
       }}
    end
  end

  @impl true
  def current_version(state) do
    with {:ok, versions} <- list_object_versions(state),
         {:ok, %{etag: etag, version_id: version}} <- find_latest(versions) do
      Logger.info(
        "found latest version of s3://#{state.bucket}/#{state.key}: #{etag} with version: #{version}"
      )

      {:ok, etag}
    else
      {:error, {:unexpected_response, %{body: reason}}} ->
        {:error, reason}

      {:error, :not_found} ->
        {:error, "could not find s3://#{state.bucket}/#{state.key}"}

      {:error, reason} ->
        Logger.error("#{__MODULE__} - unknown error: #{inspect(reason)}")
        {:error, "Unknown error"}
    end
  end

  @impl true
  def download(state) do
    Logger.info("downloading s3://#{state.bucket}/#{state.key}...")

    with {:ok, %{body: body}} <- get_object(state) do
      Logger.debug("downloaded s3://#{state.bucket}/#{state.key}!")
      {:ok, body}
    else
      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp list_object_versions(state) do
    res =
      state.bucket
      |> ExAws.S3.get_bucket_object_versions(prefix: state.key)
      |> aws_client_request(state.region)

    with {:ok, %{body: %{versions: versions}}} <- res do
      {:ok, versions}
    end
  end

  defp get_object(state) do
    state.bucket
    |> ExAws.S3.get_object(state.key)
    |> aws_client_request(state.region)
  end

  defp find_latest([_ | _] = contents) do
    dbg contents
    Enum.find(contents, fn
      %{is_latest: "true"} ->
        true

      _ ->
        false
    end)
    |> case do
      res when is_map(res) -> {:ok, res}
      _ -> {:error, :not_found}
    end
  end

  defp find_latest(_), do: {:error, :not_found}

  defp aws_client_request(op, region) do
    client().request(op, region: region)
  end

  defp client, do: Application.get_env(:remote_persistent_term, :aws_client, ExAws)
end
