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
    ],
    auto_decompress?: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Automatidally decompress the term after downloading it if known magic bytes of a \
      supported format are encountered.

      Currently only supports gzip (0x1F, 0x8B).
      """
    ],
    version_fallback?: [
      type: :boolean,
      required: false,
      default: false,
      doc: """
      If true, when deserialization fails, the system will attempt to use previous versions \
      of the term until a valid version is found or all versions are exhausted. \
      Only currently supported by the S3 fetcher.
      """
    ]
  ]

  @type t :: %__MODULE__{
          fetcher_mod: module(),
          fetcher_state: term(),
          refresh_interval: pos_integer(),
          current_version: String.t(),
          auto_decompress?: boolean(),
          version_fallback?: boolean()
        }
  defstruct [
    :fetcher_mod,
    :fetcher_state,
    :refresh_interval,
    :current_version,
    :name,
    :auto_decompress?,
    :version_fallback?
  ]

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
        with {:ok, valid_opts} <- RemotePersistentTerm.validate_options(opts),
             :ok <- setup(valid_opts) do
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
            name: name(opts),
            auto_decompress?: opts[:auto_decompress?],
            version_fallback?: opts[:version_fallback?]
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
        {:noreply, do_update_term(state)}
      end

      @impl GenServer
      def handle_info(:update, state) do
        {:noreply, do_update_term(state)}
      end

      @impl RemotePersistentTerm
      def setup(_opts), do: :ok
      defoverridable setup: 1

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

        RemotePersistentTerm.schedule_update(self(), state.refresh_interval)

        %{state | current_version: version}
      end

      defp name(opts) do
        opts[:alias] || __MODULE__ |> to_string |> String.split(".") |> tl() |> Enum.join(".")
      end
    end
  end

  @doc """
  Schedule an update of the persistent_term.
  """
  def schedule_update(pid, refresh_interval) when is_integer(refresh_interval) and is_pid(pid) do
    Process.send_after(pid, :update, refresh_interval)
  end

  def schedule_update(_pid, _refresh_interval), do: nil

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

  @doc """
  An optional, overridable callback that is executed during `start_link/1`.

  Receives the validated options passed to `start_link/1` and can be used to set up
  any additional state.

  For example, if your term is large and expected to change often, you might want to
  consider storing it in a different backend like `:ets`.

  This can be achieved by overidding `setup/1`, `put/1` and defining a custom `get/1` function
  in your module.
  """
  @callback setup(opts :: Keyword.t()) :: :ok | {:error, term()}

  @doc false
  def update_term(state, deserialize_fun, put_fun) do
    start_meta = %{name: state.name}

    :telemetry.span(
      [:remote_persistent_term, :update],
      start_meta,
      fn ->
        {status, version} =
          with {:ok, current_version, updated_fetcher_state} <-
                 state.fetcher_mod.current_version(state.fetcher_state),
               true <- state.current_version != current_version,
               :ok <-
                 download_and_store_term(
                   %{state | fetcher_state: updated_fetcher_state},
                   deserialize_fun,
                   put_fun
                 ) do
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
    # could possible enforce that the Fetcher behaviour is implemented too...
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
         {:ok, decompressed} <- maybe_decompress(state, term) do
      try_deserialize_and_store(state, decompressed, deserialize_fun, put_fun)
    end
  end

  defp try_deserialize_and_store(state, term, deserialize_fun, put_fun) do
    case deserialize_fun.(term) do
      {:ok, deserialized} ->
        put_fun.(deserialized)

      {:error, _reason} when state.version_fallback? ->
        Logger.error(
          "#{state.name} - failed to deserialize remote term, falling back to previous version"
        )

        try_previous_version(state, deserialize_fun, put_fun)

      error ->
        error
    end
  end

  defp try_previous_version(state, deserialize_fun, put_fun) do
    case state.fetcher_mod.previous_version(state.fetcher_state) do
      {:ok, previous_state} ->
        download_and_store_term(
          %{state | fetcher_state: previous_state},
          deserialize_fun,
          put_fun
        )

      {:error, _} = error ->
        error
    end
  end

  defp maybe_decompress(%__MODULE__{auto_decompress?: true}, body) do
    case body do
      <<0x1F, 0x8B, _rest::binary>> = gzipped ->
        gunzip(gzipped)

      _ ->
        {:ok, body}
    end
  end

  defp maybe_decompress(_, body), do: {:ok, body}

  defp gunzip(body) do
    {:ok, :zlib.gunzip(body)}
  rescue
    e -> {:error, {"invalid gzip data", e}}
  end
end
