defmodule RemotePersistentTerm do
  @moduledoc """
  Periodically fetch data from a remote source and store it in a [persistent_term](https://www.erlang.org/doc/man/persistent_term.html).

  `use` this module to define a GenServer that will manage the state of your fetcher and periodically
  """
  alias RemotePersistentTerm.Fetcher
  require Logger

  @opts_schema [
    remote_type: [
      type: {:in, [:s3]},
      required: true,
      default: :s3,
      doc: """
      The type of remote source in which the term is stored. Only `:s3` \
      is supported at this time."
      """
    ],
    fetcher_opts: [
      type: :keyword_list,
      required: true,
      doc: """
      Configuration options for the chosen fetcher implementation. \
      See your chosen implementation module for details."
      """
    ],
    refresh_interval: [
      type: {:or, [:pos_integer, nil]},
      required: false,
      default: nil,
      doc: """
      How often the term should be updated in milliseconds. To disable automatic refresh, \
      set the value to `nil`.
      Note: updating persistent_terms can be very expensive. \
      See [the docs](https://www.erlang.org/doc/man/persistent_term.html) for more info."
      """
    ],
    lazy_init?: [
      type: :boolean,
      required: false,
      default: false,
      doc: """
      If true, the GenServer will start up immediately and the term will be \
      populated in a `handle_continue/2`. 
      This means that there will be a period while the remote term is being \
      downloaded and no data is available. If this is not acceptable, set this \
      value to `false` (the default).
      """
    ]
  ]

  @type t :: %__MODULE__{
          fetcher_mod: module(),
          fetcher_state: term(),
          refresh_interval: pos_integer(),
          current_version: String.t()
        }
  defstruct [:fetcher_mod, :fetcher_state, :refresh_interval, :current_version]

  @doc """
  Define a GenServer that will manage this specific persistent_term.

  Example:

    This will define a GenServer that should be placed in your supervision tree.
    The GenServer will check for a new version of `s3://my-bucket/my-object` every
    12 hours and store it in a persistent_term.

    ```
    defmodule MyRemotePterm do
      use RemotePersistentTerm, 
        remote_type: :s3,
        fetcher_opts: [bucket: "my-bucket", key: "my-object"],
        refresh_interval: :timer.hours(12)
    end
    ```

  Options:
  #{NimbleOptions.docs(@opts_schema)}
  """
  defmacro __using__(opts) do
    valid_opts = NimbleOptions.validate!(opts, @opts_schema)
    fetcher_mod = RemotePersistentTerm.fetcher_mod(valid_opts[:fetcher])
    name = __MODULE__ |> to_string |> String.split(".") |> List.last() |> Macro.underscore()

    quote do
      use GenServer

      def start_link(_) do
        GenServer.start_link(__MODULE__, [], name: __MODULE__)
      end

      @impl GenServer
      def init(_) do
        fetcher_state = fetcher_mod.init(unquote(valid_opts[:fetcher_opts]))

        state = %__MODULE__{
          fetcher_mod: unquote(fetcher_mod),
          fetcher_state: fetcher_state,
          refresh_interval: unquote(valid_opts[:refresh_interval])
        }

        if unquote(valid_opts[:lazy_init?]) do
          {:ok, state, {:continue, :fetch_term}}
        else
          state =
            update_term(unquote(name), state.fetcher, state.fetcher_state, state.current_version)

          {:ok, state}
        end
      end

      @impl GenServer
      def handle_continue(:fetch_term, state) do
        state =
          update_term(unquote(name), state.fetcher, state.fetcher_state, state.current_version)

        {:noreply, state}
      end

      @impl GenServer
      def handle_info(:update, state), do: {:noreply, update_term(state)}

      @spec get() :: term()
      def get, do: :persistent_term.get(__MODULE__)
      defoverridable get: 1

      @spec put(term()) :: :ok
      def put(term), do: :persistent_term.put(__MODULE__, term)
      defoverridable put: 1

      @spec deserialize(binary()) :: {:ok, term()} | {:error, reason}
      def deserialize(binary), do: {:ok, binary}
      defoverridable deserialize: 1

      def update_term(state, fetcher, fetcher_state, prev_version) do
        version =
          RemotePersistentTerm.update_term(
            unquote(name),
            fetcher,
            fetcher_state,
            prev_version,
            &deserialize/1,
            &put/1
          )

        %{state | current_version: version}
      end
    end
  end

  def update_term(name, fetcher, fetcher_state, prev_version, deserialize_fun, put_fun) do
    with {:ok, current_version} <- fetcher.current_version(fetcher_state),
         true <- prev_version != current_version,
         :ok <- download_and_store_term(fetcher, fetcher_state, deserialize_fun, put_fun) do
      current_version
    else
      false ->
        Logger.info("#{name} - up to date")
        prev_version

      {:error, reason} ->
        Logger.error("#{name} - failed to update remote term, reason: #{inspect(reason)}")

        prev_version
    end
  end

  @doc false
  def fetcher_mod(_type = :s3), do: Fetcher.S3

  defp download_and_store_term(fetcher, fetcher_state, deserialize_fun, put_fun) do
    with {:ok, term} <- fetcher.download(fetcher_state),
         {:ok, deserialized} <- deserialize_fun.(term) do
      put_fun.(deserialized)
    end
  end
end
