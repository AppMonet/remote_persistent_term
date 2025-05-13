defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import Mox
  alias RemotePersistentTerm.Fetcher.S3
  setup :verify_on_exit!
  import ExUnit.CaptureLog

  @bucket "test-bucket"
  @key "test-key"
  @region "test-region"
  @failover_buckets [
    [bucket: "failover-bucket-1", region: "failover-region-1"],
    [bucket: "failover-bucket-2", region: "failover-region-2"]
  ]
  @version "F76V.weh4uOlU15f7a2OLHPgCLXkDpm4"

  test "Unknown error returns an error for current_version/1" do
    expect(AwsClientMock, :request, fn _op, _opts ->
      {:error, :unknown_error}
    end)

    log =
      capture_log(fn ->
        assert {:error, "Unknown error"} =
                 S3.current_version(%S3{bucket: "bucket", key: "key"})
      end)

    assert log =~ "bucket: \"bucket\""
    assert log =~ "key: \"key\""
    assert log =~ "reason: \":unknown_error\""
    assert log =~ "Failed to get current version of object - unknown reason"
  end

  describe "init/1" do
    test "example" do
      bucket = "my-bucket"
      key = "my-key"
      region = "my-region"

      assert {:ok, %S3{bucket: bucket, key: key, region: region}} ==
               S3.init(bucket: bucket, key: key, region: region)
    end

    test "with failover buckets" do
      bucket = "my-bucket"
      key = "my-key"
      region = "my-region"

      failover_buckets = [
        [bucket: "backup-bucket", region: "backup-region"],
        [bucket: "dr-bucket", region: "dr-region"]
      ]

      assert {:ok,
              %S3{bucket: bucket, key: key, region: region, failover_buckets: failover_buckets}} ==
               S3.init(
                 bucket: bucket,
                 key: key,
                 region: region,
                 failover_buckets: failover_buckets
               )
    end
  end

  describe "failover_buckets" do
    test "current_version/1 tries first failover bucket when primary bucket fails" do
      # Setup state with failover buckets
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_buckets: @failover_buckets
      }

      # Mock the AWS client to fail for primary bucket but succeed for first failover bucket
      expect(AwsClientMock, :request, 2, fn operation, opts ->
        op_bucket = operation.bucket
        region = Keyword.get(opts, :region)

        cond do
          op_bucket == @bucket && region == @region ->
            {:error, "Primary bucket connection error"}

          op_bucket == "failover-bucket-1" && region == "failover-region-1" ->
            {:ok,
             %{
               body: %{
                 versions: [
                   %{version_id: @version, etag: "current-etag", is_latest: "true"}
                 ]
               }
             }}

          true ->
            {:error, "Unexpected bucket or region"}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.current_version(state)
          assert {:ok, "current-etag", _updated_state} = result
        end)

      assert log =~ "bucket: \"#{@bucket}\""
      assert log =~ "key: \"#{@key}\""
      assert log =~ "region: \"#{@region}\""
      assert log =~ "Failed to fetch from primary bucket, attempting failover buckets"
      assert log =~ "bucket: \"failover-bucket-1\""
      assert log =~ "region: \"failover-region-1\""
      assert log =~ "Trying failover bucket"
      assert log =~ "Found latest version of object"
    end

    test "download/1 tries first failover bucket when primary bucket fails" do
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_buckets: @failover_buckets
      }

      # Mock the AWS client to fail for primary bucket but succeed for first failover bucket
      expect(AwsClientMock, :request, 2, fn operation, opts ->
        op_bucket = operation.bucket
        region = Keyword.get(opts, :region)

        cond do
          op_bucket == @bucket && region == @region ->
            {:error, "Primary bucket connection error"}

          op_bucket == "failover-bucket-1" && region == "failover-region-1" ->
            {:ok, %{body: "content from failover bucket"}}

          true ->
            {:error, "Unexpected bucket or region"}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.download(state)
          assert {:ok, "content from failover bucket"} = result
        end)

      assert log =~ "bucket: \"#{@bucket}\""
      assert log =~ "key: \"#{@key}\""
      assert log =~ "Downloading object from S3"
      assert log =~ "region: \"#{@region}\""
      assert log =~ "Failed to fetch from primary bucket, attempting failover buckets"
      assert log =~ "bucket: \"failover-bucket-1\""
      assert log =~ "region: \"failover-region-1\""
      assert log =~ "Trying failover bucket"
      assert log =~ "Downloaded object from S3"
    end

    test "returns error when primary and all failover buckets fail" do
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_buckets: @failover_buckets
      }

      # Mock the AWS client to fail for all buckets
      expect(AwsClientMock, :request, 3, fn operation, opts ->
        op_bucket = operation.bucket
        region = Keyword.get(opts, :region)

        cond do
          op_bucket == @bucket && region == @region ->
            {:error, "Primary bucket connection error"}

          op_bucket == "failover-bucket-1" && region == "failover-region-1" ->
            {:error, "First failover bucket connection error"}

          op_bucket == "failover-bucket-2" && region == "failover-region-2" ->
            {:error, "Second failover bucket connection error"}

          true ->
            {:error, "Unexpected bucket or region"}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.download(state)
          assert {:error, message} = result
          assert message =~ "All buckets failed"
        end)

      assert log =~ "bucket: \"#{@bucket}\""
      assert log =~ "key: \"#{@key}\""
      assert log =~ "Downloading object from S3"
      assert log =~ "region: \"#{@region}\""
      assert log =~ "Failed to fetch from primary bucket, attempting failover buckets"
      assert log =~ "bucket: \"failover-bucket-1\""
      assert log =~ "region: \"failover-region-1\""
      assert log =~ "Trying failover bucket"
      assert log =~ "reason: \"\\\"First failover bucket connection error\\\"\""
      assert log =~ "Failed to fetch from failover bucket"
      assert log =~ "bucket: \"failover-bucket-2\""
      assert log =~ "region: \"failover-region-2\""
      assert log =~ "reason: \"\\\"Second failover bucket connection error\\\"\""
    end

    test "tries second failover bucket when first failover bucket fails" do
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_buckets: @failover_buckets
      }

      # Mock the AWS client to fail for primary and first failover bucket but succeed for second failover bucket
      expect(AwsClientMock, :request, 3, fn operation, opts ->
        op_bucket = operation.bucket
        region = Keyword.get(opts, :region)

        cond do
          op_bucket == @bucket && region == @region ->
            {:error, "Primary bucket connection error"}

          op_bucket == "failover-bucket-1" && region == "failover-region-1" ->
            {:error, "First failover bucket connection error"}

          op_bucket == "failover-bucket-2" && region == "failover-region-2" ->
            {:ok, %{body: "content from second failover bucket"}}

          true ->
            {:error, "Unexpected bucket or region"}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.download(state)
          assert {:ok, "content from second failover bucket"} = result
        end)

      assert log =~ "bucket: \"#{@bucket}\""
      assert log =~ "key: \"#{@key}\""
      assert log =~ "Downloading object from S3"
      assert log =~ "region: \"#{@region}\""
      assert log =~ "Failed to fetch from primary bucket, attempting failover buckets"
      assert log =~ "bucket: \"failover-bucket-1\""
      assert log =~ "region: \"failover-region-1\""
      assert log =~ "Trying failover bucket"
      assert log =~ "reason: \"\\\"First failover bucket connection error\\\"\""
      assert log =~ "Failed to fetch from failover bucket"
      assert log =~ "bucket: \"failover-bucket-2\""
      assert log =~ "region: \"failover-region-2\""
      assert log =~ "Downloaded object from S3"
    end
  end

  describe "previous_version/1" do
    test "finds the correct previous version when given a current version ID" do
      versions = [
        %{
          version_id: "v3",
          last_modified: "2025-05-08T09:58:38.000Z",
          is_latest: "true"
        },
        %{
          version_id: "v2",
          last_modified: "2025-04-02T10:21:18.000Z",
          is_latest: "false"
        },
        %{
          version_id: "v1",
          last_modified: "2025-04-02T09:10:37.000Z",
          is_latest: "false"
        }
      ]

      expect(AwsClientMock, :request, fn operation, opts ->
        assert operation.bucket == @bucket
        assert operation.resource == "versions"
        assert operation.params == [prefix: @key]
        assert opts == [region: @region]
        {:ok, %{body: %{versions: versions}}}
      end)

      state = %S3{bucket: @bucket, key: @key, region: @region, version_id: "v3"}
      assert {:ok, %{version_id: "v2"}} = S3.previous_version(state)
    end

    test "returns error when there are no previous versions" do
      versions = [
        %{
          version_id: "v1",
          last_modified: "2025-04-02T09:10:37.000Z",
          is_latest: "true"
        }
      ]

      expect(AwsClientMock, :request, fn operation, opts ->
        assert operation.bucket == @bucket
        assert operation.resource == "versions"
        assert operation.params == [prefix: @key]
        assert opts == [region: @region]
        {:ok, %{body: %{versions: versions}}}
      end)

      state = %S3{bucket: @bucket, key: @key, region: @region, version_id: "v1"}
      assert {:error, :no_previous_version} = S3.previous_version(state)
    end
  end
end
