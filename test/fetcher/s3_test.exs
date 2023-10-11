defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import Mox
  alias RemotePersistentTerm.Fetcher.S3
  setup :verify_on_exit!
  import ExUnit.CaptureLog

  test "Unknown error returns an error for current_version/1" do
    expect(AwsClientMock, :request, fn _op ->
      {:error, :unknown_error}
    end)

    assert capture_log(fn ->
             assert {:error, "Unknown error"} = S3.current_version(%{bucket: "bucket"})
           end) =~
             "Elixir.RemotePersistentTerm.Fetcher.S3 - unknown error: :unknown_error"
  end
end
