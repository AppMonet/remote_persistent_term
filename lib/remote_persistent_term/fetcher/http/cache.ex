defmodule RemotePersistentTerm.Fetcher.Http.Cache do
  @moduledoc """
  A module to assist in following the HTTP Caching spec.

  See more: https://developer.mozilla.org/en-US/docs/Web/HTTP/Caching
  """

  @max_age_regex ~r/max-age=(\d+)/
  @default_age 0

  @doc """
  Calculates the refresh interval from the cache-control and age headers.
  Caching directives are case-insensitive. Multiple directives are permitted and must be comma-separated

  Refresh interval is in milliseconds
  """
  @spec refresh_interval(Req.Response.t()) :: {:ok, pos_integer()} | {:error, term()}
  def refresh_interval(resp) do
    with {:ok, max_age} <- max_age(resp) do
      age = age(resp)
      refresh_interval = (max_age - age) |> validate_refresh_interval() |> :timer.seconds()
      {:ok, refresh_interval}
    end
  end

  defp max_age(resp) do
    with [cache_control | _] <- Req.Response.get_header(resp, "cache-control"),
         true <- is_binary(cache_control) do
      value = String.downcase(cache_control)

      case Regex.run(@max_age_regex, value) do
        [_, max_age_str] ->
          {:ok, String.to_integer(max_age_str)}

        _ ->
          {:error, "max-age not found in cache-control header"}
      end
    else
      _ ->
        {:error, "cache-control header not found"}
    end
  end

  defp age(resp) do
    case Req.Response.get_header(resp, "age") do
      [age] when is_binary(age) -> String.to_integer(age)
      _ -> @default_age
    end
  end

  defp validate_refresh_interval(ri) do
    if ri < 0, do: 0, else: ri
  end
end
