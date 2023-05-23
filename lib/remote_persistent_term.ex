defmodule RemotePersistentTerm do
  @moduledoc """
  Periodically fetch data from a remote source and store it in a [persistent_term](https://www.erlang.org/doc/man/persistent_term.html).

  `use` this module to define a GenServer that will manage the state of your fetcher and periodically
  """
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

  defstruct [:fetcher_mod, :fetcher_state, :refresh_interval]

  @doc """
  Define a GenServer that will manage this specific persistent_term.

  Example:
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
    fetcher = Keyword.fetch!(opts, :fetcher)
    valid_opts = NimbleOptions.validate!(opts, @opts_schema)
    name = __MODULE__ |> to_string |> String.split(".") |> List.last() |> Macro.underscore()

    quote do
      use GenServer

      def start_link(_) do
        GenServer.start_link(__MODULE__, [], name: __MODULE__)
      end

      @impl GenServer
      def init(_) do
        fetcher_mod = fetcher_mod(unquote(valid_opts[:fetcher]))
        fetcher_opts = fetcher_mod.init(unquote(valid_opts[:fetcher_opts]))

        state = %__MODULE__{
          fetcher_mod: fetcher_mod,
          fetcher_opts: fetcher_opts,
          refresh_interval: unquote(valid_opts[:refresh_interval])
        }

        if unquote(valid_opts[:lazy_init?]) do
          {:ok, state}
        else
          {:ok, update_term()}
        end
      end

      @impl GenServer
      def handle_continue(_, state) do
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

      def update_term(prev_version \\ nil) do
        RemotePersistentTerm.update_term(
          unquote(fetcher),
          unquote(name),
          prev_version,
          &put/1
        )
      end
    end
  end

  def update_term(module, fetcher, fetcher_state, prev_version, put_fun) do
    case fetcher.current_version(fetcher_state) do
      {:ok, current_version} ->
        if prev_version != current_version do
          case fetcher.download(fetcher_state) do
            {:ok, data} ->
              put_fun.(data)

            {:error, reason} ->
              Logger.error("#{module} - failed to fetch remote term, reason: #{inspect(reason)}")
          end

          current_version
        else
          prev_version
        end

      {:error, reason} ->
        Logger.error(
          "#{module} - failed to fetch current version of remote term, reason: #{inspect(reason)}"
        )

        prev_version
    end
  end
end
