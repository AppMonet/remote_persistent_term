defmodule RemotePersistentTerm.Fetcher.S3 do
  @moduledoc """
  A Fetcher implementation for AWS S3.

  ## Versioned vs. non-versioned buckets

  This fetcher works with both versioned and non-versioned buckets. It uses the object's
  `ETag` as a change token and performs conditional GETs with `If-None-Match` to avoid
  re-downloading unchanged data.

  - **Versioned buckets**: `HEAD`/`GET` responses include `ETag`; the fetcher uses it for
    change detection. The latest object is always whatever S3 returns for the key (no explicit
    version ID required).
  - **Non-versioned buckets**: only `ETag` is available, which is sufficient to detect
    content changes. Overwriting an object with identical bytes may keep the same `ETag`,
    which is fine because the content is unchanged.

  ## S3-compatible services

  S3-compatible providers (e.g., DigitalOcean Spaces, Linode Object Storage) should work
  as long as they support standard S3 headers: `ETag`, `If-None-Match`, and `304 Not Modified`.
  If a provider ignores conditional requests, the fetcher will still function but will
  download on every refresh.
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
          failover_buckets: [failover_bucket] | nil
        }
  defstruct [:bucket, :key, :region, :failover_buckets]

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
    with {:ok, %{headers: headers}} <- head_object(state),
         {:ok, version} <- extract_version(headers) do
      Logger.info(
        bucket: state.bucket,
        key: state.key,
        version: version,
        message: "Found latest version of object"
      )

      {:ok, version}
    else
      {:error, {:http_error, 404, _}} ->
        {:error, "could not find s3://#{state.bucket}/#{state.key}"}

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

  @impl true
  def download_if_changed(state, current_version) do
    res =
      get_object_request(
        state,
        if_none_match_opts(current_version),
        &failover_on_error?/1
      )

    case res do
      {:ok, %{status_code: 304}} ->
        {:not_modified, current_version}

      {:error, {:http_error, 304, _}} ->
        {:not_modified, current_version}

      {:ok, %{body: body, headers: headers}} ->
        with {:ok, version} <- extract_version(headers) do
          {:ok, body, version}
        end

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp get_object(state) do
    get_object_request(state, [])
  end

  defp get_object_request(state, opts, failover_on_error? \\ fn _ -> true end) do
    aws_client_request(
      fn bucket, request_opts -> ExAws.S3.get_object(bucket, state.key, request_opts) end,
      state,
      opts,
      failover_on_error?
    )
  end

  defp head_object(state) do
    aws_client_request(&ExAws.S3.head_object/2, state, state.key)
  end

  defp extract_version(headers) do
    case header_value(headers, "etag") do
      nil -> {:error, :not_found}
      value -> {:ok, normalize_etag(value)}
    end
  end

  defp header_value(headers, name) do
    Enum.find_value(headers, fn
      {key, value} when is_binary(key) and is_binary(value) ->
        if key == name, do: value, else: nil

      _ ->
        nil
    end)
  end

  defp normalize_etag(value) when is_binary(value) do
    value
    |> String.trim()
    |> String.trim("\"")
  end

  defp if_none_match_opts(nil), do: []
  defp if_none_match_opts(etag), do: [if_none_match: quote_etag(etag)]

  defp quote_etag(etag) do
    etag = String.trim(etag)

    if String.starts_with?(etag, "\"") and String.ends_with?(etag, "\"") do
      etag
    else
      "\"#{etag}\""
    end
  end

  defp failover_on_error?({:http_error, 304, _}), do: false
  defp failover_on_error?(_reason), do: true

  defp aws_client_request(op, state, opts) do
    aws_client_request(op, state, opts, fn _ -> true end)
  end

  defp aws_client_request(op, %{failover_buckets: nil} = state, opts, _failover_on_error?) do
    perform_request(op, state.bucket, state.region, opts)
  end

  defp aws_client_request(
         op,
         %{
           failover_buckets: [_ | _] = failover_buckets
         } = state,
         opts,
         failover_on_error?
       ) do
    case perform_request(op, state.bucket, state.region, opts) do
      {:error, reason} = error ->
        if failover_on_error?.(reason) do
          Logger.error(%{
            bucket: state.bucket,
            key: state.key,
            region: state.region,
            reason: inspect(reason),
            message: "Failed to fetch from primary bucket, attempting failover buckets"
          })

          try_failover_buckets(op, failover_buckets, opts, state)
        else
          error
        end

      result ->
        result
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

  defp perform_request(op, bucket, region, opts) do
    op.(bucket, opts)
    |> client().request(region: region)
  end

  defp client, do: Application.get_env(:remote_persistent_term, :aws_client, ExAws)
end
