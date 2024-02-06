defmodule RemotePersistentTerm.Fetcher.HttpTest do
  use ExUnit.Case, async: true

  alias RemotePersistentTerm.Fetcher.Http

  setup do
    bypass = Bypass.open()
    [bypass: bypass, url: "http://localhost:#{bypass.port}"]
  end

  test "Should respond with 200 and data for normal endpoint", c do
    Bypass.expect(c.bypass, "GET", "/ping", fn conn ->
      Plug.Conn.resp(conn, 200, "pong")
    end)

    {:ok, state} = Http.init(url: "#{c.url}/ping")
    assert {:ok, "pong"} == Http.download(state)
    assert {:ok, "pong"} == Http.download(state)
  end

  test "An error code should result in an error", c do
    Bypass.expect(c.bypass, "GET", "/ping", fn conn ->
      Plug.Conn.resp(conn, 404, "")
    end)

    {:ok, state} = Http.init(url: "#{c.url}/ping")
    assert {:error, _} = Http.download(state)
  end
end
