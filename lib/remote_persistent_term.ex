defmodule RemotePersistentTerm do
  @moduledoc """
  Periodically fetch data from a remote source and store it in a [persistent_term](https://www.erlang.org/doc/man/persistent_term.html).

  `use` this module to define a GenServer that will manage the state of your fetcher and keep your term up to date.
  """
  alias RemotePersistentTerm.Fetcher
  require Logger

  @opts_schema [
    fetcher_mod: [
      type: {:custom, __MODULE__, :existing_module?, []},
      required: true,
      default: Fetcher.S3,
      doc: """
      The implementation of the `RemotePersistentTerm.Fetcher` behaviour which \
      should be used. Either one of the built in modules or a custom module.
      """
    ],
    fetcher_opts: [
      type: :keyword_list,
      required: false,
      default: [],
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
    refresh_timeout: [
      type: :pos_integer,
      required: false,
      default: :timer.minutes(5),
      doc: """
      When manually refreshing the term via the `update/0` function, this timeout will be \
      passed to `GenServer.call/3`.
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

    Define the module:
    ```
    defmodule MyRemotePterm do
      use RemotePersistentTerm
    end
    ```
    In your supervision tree:
    ```
    {MyRemotePterm,
     [
       fetcher_mod: RemotePersistentTerm.Fetcher.S3,
       fetcher_opts: [bucket: "my-bucket", key: "my-object"],
       refresh_interval: :timer.hours(12)
     ]}
    ```

  Options:
  #{NimbleOptions.docs(@opts_schema)}
  """
  defmacro __using__(_opts) do
    name = __MODULE__ |> to_string |> String.split(".") |> List.last() |> Macro.underscore()

    quote do
      use GenServer

      def start_link(opts) do
        with {:ok, valid_opts} <- RemotePersistentTerm.validate_options(opts) do
          GenServer.start_link(__MODULE__, valid_opts, name: __MODULE__)
        end
      end

      @impl GenServer
      def init(opts) do
        fetcher_mod = opts[:fetcher_mod]

        state = %RemotePersistentTerm{
          fetcher_mod: fetcher_mod,
          fetcher_state: fetcher_mod.init(opts[:fetcher_opts]),
          refresh_interval: opts[:refresh_interval]
        }

        if opts[:lazy_init?] do
          {:ok, state, {:continue, :fetch_term}}
        else
          {:ok, do_update_term(state)}
        end
      end

      @impl GenServer
      def handle_continue(:fetch_term, state) do
        {:noreply, do_update_term(state)}
      end

      @impl GenServer
      def handle_call(:update, _, state) do
        {:reply, do_update_term(state)}
      end

      @spec get() :: term()
      def get, do: :persistent_term.get(__MODULE__)
      defoverridable get: 0

      @spec put(term()) :: :ok
      def put(term), do: :persistent_term.put(__MODULE__, term)
      defoverridable put: 1

      @spec deserialize(binary()) :: {:ok, term()} | {:error, term()}
      def deserialize(binary), do: {:ok, binary}
      defoverridable deserialize: 1

      @doc """
      Immediately update the term.
      """
      def update, do: GenServer.call(__MODULE__, :update, :timer.minutes(5_000))

      defp do_update_term(state) do
        version =
          RemotePersistentTerm.update_term(
            unquote(name),
            state.fetcher_mod,
            state.fetcher_state,
            state.current_version,
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
  def existing_module?(value) do
    case Code.ensure_compiled(value) do
      {:module, ^value} ->
        {:ok, value}

      _ ->
        {:error, "#{__MODULE__} does not exist"}
    end
  end

  @doc false
  def validate_options(opts), do: NimbleOptions.validate(opts, @opts_schema)

  defp download_and_store_term(fetcher, fetcher_state, deserialize_fun, put_fun) do
    with {:ok, term} <- fetcher.download(fetcher_state),
         {:ok, deserialized} <- deserialize_fun.(term) do
      put_fun.(deserialized)
    end
  end
end
