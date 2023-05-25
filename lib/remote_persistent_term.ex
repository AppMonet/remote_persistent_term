defmodule RemotePersistentTerm do
  @moduledoc """
  Fetch data from a remote source and store it in a [persistent_term](https://www.erlang.org/doc/man/persistent_term.html).

  Can be configured to periodically check for updates.

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
    ],
    alias: [
      type: :atom,
      required: false,
      doc: """
      An alias for this term. A value will be generated based on the module \
      name if no value is provided. Used for Telemetry events.
      """
    ]
  ]

  @type t :: %__MODULE__{
          fetcher_mod: module(),
          fetcher_state: term(),
          refresh_interval: pos_integer(),
          current_version: String.t()
        }
  defstruct [:fetcher_mod, :fetcher_state, :refresh_interval, :current_version, :name]

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
    quote do
      use GenServer
      @behaviour RemotePersistentTerm

      def start_link(opts) do
        with {:ok, valid_opts} <- RemotePersistentTerm.validate_options(opts) do
          GenServer.start_link(__MODULE__, valid_opts, name: __MODULE__)
        end
      end

      @impl GenServer
      def init(opts) do
        fetcher_mod = opts[:fetcher_mod]

        with {:ok, fetcher_state} <- fetcher_mod.init(opts[:fetcher_opts]) do
          state = %RemotePersistentTerm{
            fetcher_mod: fetcher_mod,
            fetcher_state: fetcher_state,
            refresh_interval: opts[:refresh_interval],
            name: name(opts)
          }

          if opts[:lazy_init?] do
            {:ok, state, {:continue, :fetch_term}}
          else
            {:ok, do_update_term(state)}
          end
        end
      end

      @impl GenServer
      def handle_continue(:fetch_term, state) do
        {:noreply, do_update_term(state)}
      end

      @impl GenServer
      def handle_cast(:update, state) do
        {:reply, :ok, do_update_term(state)}
      end

      @impl GenServer
      def handle_info(:update, state) do
        {:noreply, do_update_term(state)}
      end

      @impl RemotePersistentTerm
      def get, do: :persistent_term.get(__MODULE__, nil)
      defoverridable get: 0

      @impl RemotePersistentTerm
      def put(term), do: :persistent_term.put(__MODULE__, term)
      defoverridable put: 1

      @impl RemotePersistentTerm
      def deserialize(term), do: {:ok, term}
      defoverridable deserialize: 1

      @doc "Trigger an update."
      def update, do: GenServer.cast(__MODULE__, :update)

      defp do_update_term(state) do
        version =
          RemotePersistentTerm.update_term(
            state,
            &deserialize/1,
            &put/1
          )

        if is_integer(state.refresh_interval) do
          Process.send_after(self(), :update, state.refresh_interval)
        end

        %{state | current_version: version}
      end

      defp name(opts) do
        opts[:alias] || __MODULE__ |> to_string |> String.split(".") |> tl() |> Enum.join(".")
      end
    end
  end

  @doc """
  Retrieve the currently stored term.

  Overridable.
  """
  @callback get() :: term() | nil

  @doc """
  Update the persistent_term.

  Overridable.

  This is called after `deserialize/1`.
  """
  @callback put(term()) :: :ok | {:error, term()}

  @doc """
  Deserializes the remote term, before storing it.

  Overridable.

  Commonly the remote term is an ETF encoded binary. In this case you will likely want to
  override this callback with something like:
    ```
    def deserialize(binary) do
      {:ok, :erlang.binary_to_term(binary)}
    rescue
      _ ->
        {:error, "got invalid ETF"}
    end
    ```
  """
  @callback deserialize(term()) :: {:ok, term()} | {:error, term()}

  @doc false
  def update_term(state, deserialize_fun, put_fun) do
    start_meta = %{name: state.name}

    :telemetry.span(
      [:remote_persistent_term, :update],
      start_meta,
      fn ->
        {status, version} =
          with {:ok, current_version} <- state.fetcher_mod.current_version(state.fetcher_state),
               true <- state.current_version != current_version,
               :ok <- download_and_store_term(state, deserialize_fun, put_fun) do
            {:updated, current_version}
          else
            false ->
              Logger.info("#{state.name} - up to date")
              {:not_updated, state.current_version}

            {:error, reason} ->
              Logger.error(
                "#{state.name} - failed to update remote term, reason: #{inspect(reason)}"
              )

              {:not_updated, state.current_version}
          end

        {version, Map.put(start_meta, :status, status)}
      end
    )
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

  defp download_and_store_term(state, deserialize_fun, put_fun) do
    with {:ok, term} <- state.fetcher_mod.download(state.fetcher_state),
         {:ok, deserialized} <- deserialize_fun.(term) do
      put_fun.(deserialized)
    end
  end
end
