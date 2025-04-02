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
    assert {:ok, "pong"} == Http.download(state, version())
    assert {:ok, "pong"} == Http.download(state, version())
  end

  test "An error code should result in an error", c do
    Bypass.expect(c.bypass, "GET", "/ping", fn conn ->
      Plug.Conn.resp(conn, 404, "")
    end)

    {:ok, state} = Http.init(url: "#{c.url}/ping")
    assert {:error, _} = Http.download(state, version())
  end

  test "Should schedule the next update if http_cache is enabled", c do
    Bypass.expect(c.bypass, "GET", "/ping", fn conn ->
      Plug.Conn.put_resp_header(conn, "cache-control", "max-age=0")
      |> Plug.Conn.resp(200, "pong")
    end)

    {:ok, state} =
      Http.init(url: "#{c.url}/ping", http_cache: [enabled?: true, min_refresh_interval_ms: 0])

    assert {:ok, "pong"} == Http.download(state, version())

    # this signifies that we are scheduling the next update
    assert_receive :update
  end

  test "Should NOT schedule the next update if http_cache is disabled", c do
    Bypass.expect(c.bypass, "GET", "/ping", fn conn ->
      Plug.Conn.put_resp_header(conn, "cache-control", "max-age=0")
      |> Plug.Conn.resp(200, "pong")
    end)

    # the cache should be disabled by default
    {:ok, state} = Http.init(url: "#{c.url}/ping")
    assert {:ok, "pong"} == Http.download(state, version())

    refute_receive :update
  end

  defp version do
    DateTime.utc_now() |> DateTime.to_string()
  end
end
