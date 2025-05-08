defmodule RemotePersistentTerm.Fetcher.S3 do
  @moduledoc """
  A Fetcher implementation for AWS S3.
  """
  require Logger

  @behaviour RemotePersistentTerm.Fetcher

  @type bucket :: String.t()
  @type region :: String.t()
  @type failover_bucket :: [bucket: bucket, region: region]

  @type t :: %__MODULE__{
          bucket: bucket,
          key: String.t(),
          region: region,
          failover_buckets: [failover_bucket] | nil,
          version_id: String.t() | nil
        }
  defstruct [:bucket, :key, :region, :failover_buckets, :version_id]

  @failover_bucket_schema [
    bucket: [
      type: :string,
      required: true,
      doc: "The name of the failover S3 bucket."
    ],
    region: [
      type: :string,
      required: true,
      doc: "The AWS region of the failover S3 bucket."
    ]
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
    failover_buckets: [
      type: {:list, {:keyword_list, @failover_bucket_schema}},
      required: false,
      doc: "A list of failover_buckets to use as failover if the primary bucket fails. \n
        The directory structure in failover buckets must match the primary bucket."
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
         region: valid_opts[:region],
         failover_buckets: valid_opts[:failover_buckets]
       }}
    end
  end

  @impl true
  def current_version(state) do
    with {:ok, versions} <- list_object_versions(state),
         {:ok, %{etag: etag, version_id: version}} <- find_latest(versions) do
      Logger.info(
        bucket: state.bucket,
        key: state.key,
        version: version,
        message: "Found latest version of object"
      )

      {:ok, etag}
    else
      {:error, {:unexpected_response, %{body: reason}}} ->
        {:error, reason}

      {:error, :not_found} ->
        {:error, "could not find s3://#{state.bucket}/#{state.key}"}

      {:error, reason} ->
        Logger.error(%{
          bucket: state.bucket,
          key: state.key,
          reason: inspect(reason),
          message: "Failed to get current version of object - unknown reason"
        })

        {:error, "Unknown error"}
    end
  end

  @impl true
  def download(state) do
    Logger.info(
      bucket: state.bucket,
      key: state.key,
      message: "Downloading object from S3"
    )

    with {:ok, %{body: body}} <- get_object(state) do
      Logger.debug(
        bucket: state.bucket,
        key: state.key,
        message: "Downloaded object from S3"
      )

      {:ok, body}
    else
      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp list_object_versions(state) do
    res =
      aws_client_request(
        :get_bucket_object_versions,
        state,
        [[prefix: state.key]]
      )

    with {:ok, %{body: %{versions: versions}}} <- res do
      {:ok, versions}
    end
  end

  defp get_object(state) do
    aws_client_request(:get_object, state, [state.key, [version_id: state.version_id]])
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

  @impl true
  def previous_version(state) do
    Logger.info(
      bucket: state.bucket,
      key: state.key,
      message: "About to fetch previous version of object",
      version_id: state.version_id
    )

    with {:ok, versions} <- list_object_versions(state),
         {:ok, previous_version} <- find_previous_version(versions, state.version_id) do
      {:ok, %{state | version_id: previous_version.version_id}}
    else
      {:error, reason} ->
        Logger.error(%{
          bucket: state.bucket,
          key: state.key,
          reason: inspect(reason),
          message: "Failed to get previous version of object"
        })

        {:error, reason}
    end
  end

  defp find_previous_version(versions, current_version_id) do
    versions
    |> Enum.sort_by(
      fn version ->
        {:ok, datetime, _} = DateTime.from_iso8601(version.last_modified)
        datetime
      end,
      {:desc, DateTime}
    )
    |> Enum.find(fn version ->
      version.version_id != current_version_id
    end)
    |> case do
      nil -> {:error, :no_previous_version}
      version -> {:ok, version}
    end
  end

  defp aws_client_request(op, %{failover_buckets: nil} = state, opts) do
    perform_request(op, state.bucket, state.region, opts)
  end

  defp aws_client_request(
         op,
         %{
           failover_buckets: [_ | _] = failover_buckets
         } = state,
         opts
       ) do
    with {:error, reason} <- perform_request(op, state.bucket, state.region, opts) do
      Logger.error(%{
        bucket: state.bucket,
        key: state.key,
        region: state.region,
        reason: inspect(reason),
        message: "Failed to fetch from primary bucket, attempting failover buckets"
      })

      try_failover_buckets(op, failover_buckets, opts, state)
    end
  end

  defp try_failover_buckets(_op, [], _opts, _state), do: {:error, "All buckets failed"}

  defp try_failover_buckets(
         op,
         [[bucket: bucket, region: region] | remaining_buckets],
         opts,
         state
       ) do
    Logger.info(%{
      bucket: bucket,
      key: state.key,
      region: region,
      message: "Trying failover bucket"
    })

    case perform_request(op, bucket, region, opts) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        Logger.error(%{
          bucket: bucket,
          key: state.key,
          region: region,
          reason: inspect(reason),
          message: "Failed to fetch from failover bucket"
        })

        try_failover_buckets(op, remaining_buckets, opts, state)
    end
  end

  defp perform_request(func, bucket, region, opts) do
    apply(ExAws.S3, func, [bucket | opts])
    |> client().request(region: region)
  end

  defp client, do: Application.get_env(:remote_persistent_term, :aws_client, ExAws)
end
