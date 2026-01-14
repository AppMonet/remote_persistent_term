defmodule RemotePersistentTerm.Fetcher.S3.HttpClient do
  @moduledoc """
  ExAws HTTP client implementation for Req.
  """

  @behaviour ExAws.Request.HttpClient

  @impl ExAws.Request.HttpClient
  def request(method, url, body, headers, _http_opts) do
    request = Req.new(decode_body: false, retry: false)

    case Req.request(request, method: method, url: url, body: body, headers: headers) do
      {:ok, response} ->
        response = %{
          status_code: response.status,
          headers: Req.get_headers_list(response),
          body: response.body
        }

        {:ok, response}

      {:error, reason} ->
        {:error, %{reason: reason}}
    end
  end
end
