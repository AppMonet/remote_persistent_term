defmodule RemotePersistentTermTest do
  use ExUnit.Case
  doctest RemotePersistentTerm
  import ExUnit.CaptureLog

  defmodule MyStaticFetcher do
    use RemotePersistentTerm.Fetcher.Static, data: %{my: :data}
  end

  defmodule StaticRemotePersistentTerm do
    use RemotePersistentTerm
  end

  defmodule FailingDeserializationTerm do
    use RemotePersistentTerm
    def deserialize(_), do: {:error, :invalid}
  end

  test "basic example with Static fetcher" do
    start_supervised!({StaticRemotePersistentTerm, [fetcher_mod: MyStaticFetcher]})
    StaticRemotePersistentTerm.update()
    assert %{my: :data} == StaticRemotePersistentTerm.get()
  end

  test "does not store data when deserialization fails" do
    assert capture_log(fn ->
             start_supervised!({FailingDeserializationTerm, [fetcher_mod: MyStaticFetcher]})
             assert FailingDeserializationTerm.get() |> is_nil()
           end) =~
             "RemotePersistentTermTest.FailingDeserializationTerm - failed to update remote term, reason: :invalid"
  end
end
