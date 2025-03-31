defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import Mox
  alias RemotePersistentTerm.Fetcher.S3
  setup :verify_on_exit!
  import ExUnit.CaptureLog

  test "Unknown error returns an error for current_version/1" do
    expect(AwsClientMock, :request, fn _op, _opts ->
      {:error, :unknown_error}
    end)

    assert capture_log(fn ->
             assert {:error, "Unknown error"} = S3.current_version(%S3{bucket: "bucket"})
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
      assert {:ok, "hello world!"} = S3.download(state)

      state = %S3{compression: nil}
      assert {:ok, ^data} = S3.download(state)
    end
  end

  describe "init/1" do
    test "example" do
      bucket = "my-bucket"
      key = "my-key"
      region = "my-region"

      assert {:ok, %S3{bucket: bucket, key: key, region: region, compression: :gzip}} ==
               S3.init(bucket: bucket, key: key, region: region, compression: :gzip)
    end
  end
end
