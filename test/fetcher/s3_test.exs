defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import Mox
  alias RemotePersistentTerm.Fetcher.S3
  setup :verify_on_exit!

  test "Mint.TransportError returns an error for current_version/1" do
    expect(AwsClientMock, :request, fn _op ->
      {:error, %Mint.TransportError{reason: :timeout}}
    end)

    assert {:error, :timeout} = S3.current_version(%{bucket: "bucket"})
  end

  test "Unknown error returns an error for current_version/1" do
    expect(AwsClientMock, :request, fn _op ->
      {:error, :unknown_error}
    end)

    assert {:error, "Unknown error"} = S3.current_version(%{bucket: "bucket"})
  end
end
