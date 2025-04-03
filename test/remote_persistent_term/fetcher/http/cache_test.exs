defmodule RemotePersistentTerm.Fetcher.Http.CacheTest do
  use ExUnit.Case, async: true

  alias RemotePersistentTerm.Fetcher.Http.Cache

  test "refresh_interval/2 should return a value based on the cache-control and age headers" do
    resp = %Req.Response{
      status: 200,
      body: "",
      trailers: %{},
      private: %{}
    }

    spec = [
      #  max-age minus age
      {%{
         "cache-control" => ["max-age=10000, private, must-revalidate"],
         "content-length" => ["0"],
         "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
         "server" => ["Cowboy"],
         "age" => ["1000"]
       }, 9000},
      {%{
         "cache-control" => ["max-age=10000, private, must-revalidate"],
         "content-length" => ["0"],
         "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
         "server" => ["Cowboy"]
       }, 10000},
      #  max-age in different parts of the list
      {%{
         "content-length" => ["0"],
         "cache-control" => ["max-age=50000, private, must-revalidate"],
         "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
         "server" => ["Cowboy"]
       }, 50000},
      {%{
         "content-length" => ["0"],
         "cache-control" => ["private, max-age=9990, must-revalidate"],
         "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
         "server" => ["Cowboy"]
       }, 9990},
      {%{
         "content-length" => ["0"],
         "cache-control" => ["private, must-revalidate, max-age=360000"],
         "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
         "server" => ["Cowboy"]
       }, 360_000},
      #  should handle max-age in upper-case
      {%{
         "cache-control" => ["MAX-aGe=89890"]
       }, 89890},
      # negative value should be treated as 0
      {%{
         "cache-control" => ["max-age=100, private, must-revalidate"],
         "content-length" => ["0"],
         "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
         "server" => ["Cowboy"],
         "age" => ["1000"]
       }, 0}
    ]

    Enum.map(spec, fn {headers, expected_refresh_interval} ->
      resp = Map.put(resp, :headers, headers)
      expected = :timer.seconds(expected_refresh_interval)
      assert {:ok, ^expected} = Cache.refresh_interval(resp)
    end)
  end

  test "refresh_interval/1 should return an error if max-age is not found" do
    resp = %Req.Response{
      status: 200,
      headers: %{
        "cache-control" => ["private, must-revalidate"],
        "content-length" => ["0"],
        "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
        "server" => ["Cowboy"],
        "age" => ["100"]
      },
      body: "",
      trailers: %{},
      private: %{}
    }

    assert {:error, "max-age not found in cache-control header"} = Cache.refresh_interval(resp)
  end

  test "refresh_interval/1 should return an error if cache-control header is not found" do
    resp = %Req.Response{
      status: 200,
      headers: %{
        "content-length" => ["0"],
        "date" => ["Tue, 06 Feb 2024 11:05:02 GMT"],
        "server" => ["Cowboy"],
        "age" => ["100"]
      },
      body: "",
      trailers: %{},
      private: %{}
    }

    assert {:error, "cache-control header not found"} = Cache.refresh_interval(resp)
  end
end
