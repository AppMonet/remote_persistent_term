defmodule RemotePersistentTermTest do
  use ExUnit.Case
  doctest RemotePersistentTerm

  test "greets the world" do
    assert RemotePersistentTerm.hello() == :world
  end
end
