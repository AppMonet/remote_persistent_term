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

  describe "init/1" do
    test "example" do
      bucket = "my-bucket"
      key = "my-key"
      region = "my-region"

      assert {:ok, %S3{bucket: bucket, key: key, region: region}} ==
               S3.init(bucket: bucket, key: key, region: region)
    end
  end
end
