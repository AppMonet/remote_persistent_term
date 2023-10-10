defmodule RemotePersistentTerm.Fetcher.S3Test do
  use ExUnit.Case
  import ExUnit.CaptureLog
  import Mock
  alias RemotePersistentTerm.Fetcher.S3

  test "Mint.TransportError returns an error for current_version/1" do
    with_mock ExAws, request: fn _op -> {:error, %Mint.TransportError{reason: :timeout}} end do
      {:error, reason} = S3.current_version(%{bucket: "bucket"})
      assert reason = :timeout
    end
  end
end
