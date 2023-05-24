defmodule RemotePersistentTermTest do
  use ExUnit.Case
  doctest RemotePersistentTerm

  defmodule MyStaticFetcher do
    use RemotePersistentTerm.Fetcher.Static, data: %{my: :data}
  end

  defmodule StaticRemotePersistentTerm do
    use RemotePersistentTerm
  end

  test "basic example with Static fetcher" do
    start_supervised!({StaticRemotePersistentTerm, [fetcher_mod: MyStaticFetcher]})
    assert %{my: :data} == StaticRemotePersistentTerm.get()
  end
end
