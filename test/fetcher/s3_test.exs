defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import Mox
  alias RemotePersistentTerm.Fetcher.S3
  setup :verify_on_exit!
  import ExUnit.CaptureLog

  @version "F76V.weh4uOlU15f7a2OLHPgCLXkDpm4"
  @previous_version "EarlierVersion.1234567890abcdef"
  @bucket "test-bucket"
  @key "test-key"
  @region "test-region"

  test "Unknown error returns an error for current_identifiers/1" do
    expect(AwsClientMock, :request, fn _op, _opts ->
      {:error, :unknown_error}
    end)

    assert capture_log(fn ->
             assert {:error, "Unknown error"} = S3.current_identifiers(%S3{bucket: "bucket"})
           end) =~
             "Elixir.RemotePersistentTerm.Fetcher.S3 - unknown error: :unknown_error"
  end

  describe "compression" do
    test "gzip compression" do
      data = "hello world!" |> :zlib.gzip()

      expect(AwsClientMock, :request, 2, fn _op, _opts ->
        {:ok, %{body: data}}
      end)

      state = %S3{compression: :gzip}
      assert {:ok, "hello world!"} = S3.download(state, @version)

      state = %S3{compression: nil}
      assert {:ok, ^data} = S3.download(state, @version)
    end
  end

  describe "download/2" do
    test "downloads specific version when version is provided" do
      specific_version = "specific-version-123"
      expect(AwsClientMock, :request, fn op, opts ->
        assert op.params == %{"versionId" => specific_version}
        assert opts == [region: @region]
        {:ok, %{body: "version specific content"}}
      end)

      state = %S3{bucket: @bucket, key: @key, region: @region}
      assert {:ok, "version specific content"} = S3.download(state, specific_version)
    end

    test "downloads latest version when version is nil" do
      # Verify that ExAws.S3.get_object is called without version_id parameter
      expect(AwsClientMock, :request, fn op, opts ->
        # No version ID in params
        refute Map.has_key?(op.params || %{}, "versionId")
        assert opts == [region: @region]
        {:ok, %{body: "latest content"}}
      end)

      state = %S3{bucket: @bucket, key: @key, region: @region}
      assert {:ok, "latest content"} = S3.download(state, nil)
    end
  end

  describe "init/1" do
    test "example" do
      assert {:ok,
              %S3{
                bucket: @bucket,
                key: @key,
                region: @region,
                compression: :gzip,
                fallback_to_previous_version?: false
              }} ==
               S3.init(bucket: @bucket, key: @key, region: @region, compression: :gzip)
    end
  end

  describe "retry/2" do
    test "with fallback_to_previous_version?: true returns previous version" do
      versions = [
        %{version_id: @version, etag: "current-etag", is_latest: "true"},
        %{version_id: @previous_version, etag: "previous-etag", is_latest: "false"}
      ]

      expect(AwsClientMock, :request, fn _op, _opts ->
        {:ok, %{body: %{versions: versions}}}
      end)

      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        fallback_to_previous_version?: true
      }

      assert capture_log(fn ->
               result = S3.retry(state, @version)

               assert result ==
                        {:retry_new_version, %{version: @previous_version, etag: "previous-etag"}}
             end) =~ "Retry enabled, falling back to previous version"
    end

    test "with fallback_to_previous_version?: true returns :continue when version is not found" do
      versions = [
        %{version_id: "some-other-version", etag: "other-etag", is_latest: "true"},
        %{version_id: @previous_version, etag: "previous-etag", is_latest: "false"}
      ]

      expect(AwsClientMock, :request, fn _op, _opts ->
        {:ok, %{body: %{versions: versions}}}
      end)

      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        fallback_to_previous_version?: true
      }

      assert capture_log(fn ->
               result = S3.retry(state, @version)
               assert result == :continue
             end) =~ "Could not find version"
    end

    test "with fallback_to_previous_version?: true returns :continue when version is the last one" do
      versions = [
        %{version_id: "some-other-version", etag: "other-etag", is_latest: "true"},
        %{version_id: @version, etag: "current-etag", is_latest: "false"}
      ]

      # Mock the AWS client request to return the versions
      expect(AwsClientMock, :request, fn _op, _opts ->
        {:ok, %{body: %{versions: versions}}}
      end)

      state = %S3{
        bucket: @bucket,
        key: @key,
        region: @region,
        fallback_to_previous_version?: true
      }

      result = S3.retry(state, @version)
      assert result == :continue
    end

    test "with fallback_to_previous_version?: false returns :continue" do
      state = %S3{fallback_to_previous_version?: false}
      assert S3.retry(state, @version) == :continue
    end
  end
end
