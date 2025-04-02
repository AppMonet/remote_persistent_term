defmodule RemotePersistentTerm.Fetcher.Static do
  @moduledoc """
  A macro to help define a valid `RemotePersistentTerm.Fetcher` which
  always returns some hardcoded static data.

  Mostly intended for testing purposes.

  Example:

  ```
  defmodule MyStaticFetcher do
    use RemotePersistentTerm.Fetcher.Static, data: %{my: :data}
  end
  ```
  """

  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour RemotePersistentTerm.Fetcher

      @impl true
      def init(_), do: {:ok, []}

      @impl true
      def current_identifiers(_), do: {:ok, %{version: unquote(Keyword.get(opts, :version, "1"))}}

      @impl true
      def download(_, _), do: {:ok, unquote(Macro.escape(Keyword.fetch!(opts, :data)))}

      @impl true
      def retry(_, _), do: :continue
    end
  end
end
