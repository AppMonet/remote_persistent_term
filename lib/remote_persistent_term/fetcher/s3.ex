defmodule RemotePersistentTerm.Fetcher.S3 do
  @moduledoc """
  A Fetcher implementation for AWS S3.
  """
  require Logger

  @behaviour RemotePersistentTerm.Fetcher
  @aws_client Application.compile_env(:remote_persistent_term, :aws_client, ExAws)

  @type t :: %__MODULE__{
          bucket: String.t(),
          key: String.t(),
          region: String.t(),
          compression: atom() | nil,
          failover_region: String.t() | nil,
          fallback_to_previous_version?: boolean()
        }
  defstruct [
    :bucket,
    :key,
    :region,
    :compression,
    :failover_region,
    :fallback_to_previous_version?
  ]

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
    ],
    compression: [
      type: {:in, [:gzip]},
      required: false,
      doc: "The compression algorithm used to compress the remote term."
    ],
    retry: [
      type: :keyword_list,
      required: false,
      default: [],
      doc: "Options for retry behavior when fetching fails.",
      keys: [
        failover_region: [
          type: :string,
          required: false,
          doc: "The AWS region to use if downloading from the default region fails."
        ],
        fallback_to_previous_version?: [
          type: :boolean,
          required: false,
          default: false,
          doc:
            "Whether to fallback to a previous version of the S3 object if deserialization fails."
        ]
      ]
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
      retry = Keyword.get(valid_opts, :retry, [])

      {:ok,
       %__MODULE__{
         bucket: valid_opts[:bucket],
         key: valid_opts[:key],
         region: valid_opts[:region],
         compression: valid_opts[:compression],
         failover_region: retry[:failover_region],
         fallback_to_previous_version?: retry[:fallback_to_previous_version?]
       }}
    end
  end

  @impl true
  def current_identifiers(state) do
    with {:ok, versions} <- list_object_versions(state),
         {:ok, %{etag: etag, version_id: version}} <- find_latest(versions) do
      Logger.info(
        "found latest version of s3://#{state.bucket}/#{state.key}: with etag:#{etag} and version:#{version}"
      )

      {:ok, %{etag: etag, version: version}}
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
  def download(state, version) do
    Logger.info("downloading s3://#{state.bucket}/#{state.key} with version #{version}")

    with {:ok, %{body: body}} <- get_object(state, version),
         _ <- Logger.debug("downloaded s3://#{state.bucket}/#{state.key}!"),
         {:ok, decompressed} <- decompress(state.compression, body) do
      {:ok, decompressed}
    else
      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  @impl true
  def retry(%{fallback_to_previous_version?: true} = state, version) do
    Logger.info(
      "Retry enabled, falling back to previous version as deserialization failed for s3://#{state.bucket}/#{state.key} with version #{version}"
    )

    case get_previous_version(state, version) do
      {:ok, identifiers} ->
        {:retry_new_version, identifiers}

      _ ->
        :continue
    end
  end

  def retry(_, _), do: :continue

  def get_previous_version(state, version_id) do
    with {:ok, versions} <- list_object_versions(state) do
      # Find the index of the current version
      case Enum.find_index(versions, fn v -> v.version_id == version_id end) do
        nil ->
          Logger.error(
            "Could not find version #{version_id} in the list of versions for s3://#{state.bucket}/#{state.key}"
          )

          :error

        # Final version in the list
        index when index == length(versions) - 1 ->
          :error

        index ->
          previous = Enum.at(versions, index + 1)
          {:ok, %{version: previous.version_id, etag: previous.etag}}
      end
    end
  end

  defp list_object_versions(state) do
    res =
      state.bucket
      |> ExAws.S3.get_bucket_object_versions(prefix: state.key)
      |> aws_client_request(state)

    with {:ok, %{body: %{versions: versions}}} <- res do
      {:ok, versions}
    end
  end

  defp get_object(state, version) when is_binary(version) do
    state.bucket
    |> ExAws.S3.get_object(state.key, version_id: version)
    |> aws_client_request(state)
  end

  defp get_object(state, _) do
    state.bucket
    |> ExAws.S3.get_object(state.key)
    |> aws_client_request(state)
  end

  defp find_latest([_ | _] = contents) do
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

  defp decompress(:gzip, body) do
    {:ok, :zlib.gunzip(body)}
  rescue
    e -> {:error, {"invalid gzip data", e}}
  end

  defp decompress(nil, body), do: {:ok, body}

  defp aws_client_request(op, %{region: region, failover_region: nil}),
    do: @aws_client.request(op, region: region)

  defp aws_client_request(op, %{region: region, failover_region: failover_region}) do
    with {:error, reason} <- @aws_client.request(op, region: region) do
      Logger.error(
        "Failed to fetch from primary region #{region}: #{inspect(reason)}, will try failover region #{failover_region}"
      )

      @aws_client.request(op, region: failover_region)
    end
  end
end
