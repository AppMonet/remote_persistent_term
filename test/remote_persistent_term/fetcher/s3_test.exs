defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import Mox
  alias RemotePersistentTerm.Fetcher.S3
  setup :verify_on_exit!
  import ExUnit.CaptureLog

  @bucket "test-bucket"
  @key "test-key"
  @region "test-region"
  @failover_region "failover-region"
  @version "F76V.weh4uOlU15f7a2OLHPgCLXkDpm4"

  test "Unknown error returns an error for current_version/1" do
    expect(AwsClientMock, :request, fn _op, _opts ->
      {:error, :unknown_error}
    end)

    assert capture_log(fn ->
             assert {:error, "Unknown error"} = S3.current_version(%S3{bucket: "bucket"})
           end) =~
             "Elixir.RemotePersistentTerm.Fetcher.S3 - unknown error: :unknown_error"
  end

  describe "init/1" do
    test "example" do
      bucket = "my-bucket"
      key = "my-key"
      region = "my-region"

      assert {:ok, %S3{bucket: bucket, key: key, region: region}} ==
               S3.init(bucket: bucket, key: key, region: region)
    end
  end

  describe "failover_region" do
    test "current_identifiers/1 tries failover region when primary region fails" do
      # Setup state with failover region
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_region: @failover_region
      }

      # Mock the AWS client to fail for primary region but succeed for failover region
      expect(AwsClientMock, :request, 2, fn _op, opts ->
        case opts do
          [region: @region] ->
            {:error, "Primary region connection error"}

          [region: @failover_region] ->
            {:ok,
             %{
               body: %{
                 versions: [
                   %{version_id: @version, etag: "current-etag", is_latest: "true"}
                 ]
               }
             }}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.current_version(state)
          assert {:ok, "current-etag"} = result
        end)

      assert log =~ "Failed to fetch from primary region #{@region}"
      assert log =~ "will try failover region #{@failover_region}"
    end

    test "download/1 tries failover region when primary region fails" do
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_region: @failover_region
      }

      # Mock the AWS client to fail for primary region but succeed for failover region
      expect(AwsClientMock, :request, 2, fn _op, opts ->
        case opts do
          [region: @region] ->
            {:error, "Primary region connection error"}

          [region: @failover_region] ->
            {:ok, %{body: "content from failover region"}}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.download(state)
          assert {:ok, "content from failover region"} = result
        end)

      assert log =~ "Failed to fetch from primary region #{@region}"
      assert log =~ "will try failover region #{@failover_region}"
    end

    test "returns error when both primary and failover regions fail" do
      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        failover_region: @failover_region
      }

      # Mock the AWS client to fail for both regions
      expect(AwsClientMock, :request, 2, fn _op, opts ->
        case opts do
          [region: @region] ->
            {:error, "Primary region connection error"}

          [region: @failover_region] ->
            {:error, "Failover region connection error"}
        end
      end)

      log =
        capture_log(fn ->
          result = S3.download(state)
          assert {:error, _} = result
        end)

      assert log =~ "Failed to fetch from primary region #{@region}"
      assert log =~ "will try failover region #{@failover_region}"
    end
  end
end
