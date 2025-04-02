defmodule RemotePersistentTerm.Fetcher do
  @moduledoc """
  A generic behaviour for a Fetcher module which is responsible for checking the latest
  version available in the remote source and downloading it.
  """

  @typedoc """
  Implementation specific state.
  """
  @type state :: term()
  @type opts :: Keyword.t()
  @type etag :: String.t()
  @type version :: String.t()
  @type identifiers :: %{
          etag: etag(),
          version: version()
        }

  @doc """
  Initialize the implementation specific state of the Fetcher.
  """
  @callback init(opts()) :: {:ok, state()} | {:error, term()}

  @doc """
  Check the current version of the remote term. Used to avoid downloading the
  same term multiple times.
  """
  @callback current_identifiers(state()) :: {:ok, identifiers()} | {:error, term()}

  @doc """
  Download the term from the remote source.
  """
  @callback download(state(), version()) :: {:ok, term()} | {:error, term()}

  @doc """
  Logic for whether the request be retried.
  """
  @callback retry(state(), version()) :: {:retry_new_version, version()} | :retry | :continue
end
