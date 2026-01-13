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
  @type download_if_changed_result ::
          {:ok, term(), version()} | {:not_modified, version() | nil} | {:error, term()}

  @doc """
  Initialize the implementation specific state of the Fetcher.
  """
  @callback init(opts()) :: {:ok, state()} | {:error, term()}

  @doc """
  Check the current version of the remote term. Used to avoid downloading the
  same term multiple times.
  """
  @callback current_version(state()) :: {:ok, version()} | {:error, term()}

  @doc """
  Download the term from the remote source.
  """
  @callback download(state()) :: {:ok, term()} | {:error, term()}

  @doc """
  Optionally download the term only if it has changed. When implemented, it should
  return `{:not_modified, current_version}` for an unchanged term or `{:ok, term, new_version}`.
  """
  @callback download_if_changed(state(), version() | nil) :: download_if_changed_result

  @optional_callbacks download_if_changed: 2
end
