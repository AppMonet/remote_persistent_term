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

  test "A 200 then a 304 should result in two 200 responses", c do
    # First return a 200 with the data, then 304 with empty body
    Bypass.expect(c.bypass, "GET", "/ping", fn conn ->
      if cache_hit?(conn) do
        Plug.Conn.resp(conn, 304, "")
      else
        Plug.Conn.resp(conn, 200, "pong")
      end
    end)

    assert {:ok, state} = Http.init(url: "#{c.url}/ping")
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

  defp cache_hit?(conn) do
    case Plug.Conn.get_req_header(conn, "if-modified-since") do
      [] -> false
      _ -> true
    end
  end
end
