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
  @type version :: String.t()

  @doc """
  Initialize the implementation specific state of the Fetcher.
  """
  @callback init(opts()) :: {:ok, state()} | {:error, term()}

  @doc """
  Check the current version of the remote term. Useed to avoid downloading the
  same term multiple times.
  """
  @callback current_version(state()) :: {:ok, version()} | {:error, term()}

  @doc """
  Download the term from the remote source.
  """
  @callback download(state()) :: {:ok, term()} | {:error, term()}
end
