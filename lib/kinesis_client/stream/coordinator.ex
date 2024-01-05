defmodule KinesisClient.Stream.Coordinator do
  @moduledoc """
  This module will describe a given stream and enumerate all the shards. It will then handle
  starting and stopping `KinesisClient.Stream.Shard` processes as necessary to ensure the
  stream is processed completely and in the correct order.
  """
  use GenServer
  require Logger

  alias KinesisClient.Kinesis
  alias KinesisClient.Stream.AppState
  alias KinesisClient.Stream.Shard

  defstruct [
    :name,
    :app_name,
    :app_state_opts,
    :stream_name,
    :shard_supervisor_name,
    :notify_pid,
    :shard_graph,
    :shard_map,
    :kinesis_opts,
    :retry_timeout,
    # unique reference used to identify this instance KinesisClient.Stream
    :worker_ref,
    :shard_args,
    shard_pid_map: %{},
    startup_attempt: 1
  ]

  @max_startup_attempts 3
  @jitter 1000

  @doc """
  Starts a KinesisClient.Stream.Coordinator. KinesisClient.Stream should handle starting this.

  ## Options
    * `:name` - Required. The process name.
    * `:app_name` - Required. Will be used to name the DyanmoDB table.
    * `:stream_name` - Required. The stream to describe and start Shard workers for.
    * `:shard_supervisor_name` - Required. Needed in order to start Shard workers.
  """
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: args[:name])
  end

  def append_shards(coordinator, child_shards) do
    GenServer.call(coordinator, {:append_shards, child_shards})
  end

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      name: opts[:name],
      app_name: opts[:app_name],
      app_state_opts: opts[:app_state_opts],
      stream_name: opts[:stream_name],
      shard_supervisor_name: opts[:shard_supervisor_name],
      worker_ref: opts[:worker_ref],
      shard_args: opts[:shard_args],
      notify_pid: Keyword.get(opts, :notify_pid),
      kinesis_opts: Keyword.get(opts, :kinesis_opts, []),
      retry_timeout: Keyword.get(opts, :retry_timeout, 30_000)
    }

    Logger.debug("Starting KinesisClient.Stream.Coordinator: #{inspect(state)}")
    {:ok, state, {:continue, :initialize}}
  end

  @impl GenServer
  def handle_continue(:initialize, state) do
    create_table_if_not_exists(state)
    describe_stream(state)
  end

  def handle_continue(:describe_stream, state) do
    describe_stream(state)
  end

  @impl GenServer
  def handle_call(:get_graph, _from, %{shard_graph: graph} = s) do
    {:reply, graph, s}
  end

  def handle_call({:append_shards, child_shards}, _from, state) do
    shard_pid_map =
      Enum.reduce(
        child_shards,
        state.shard_pid_map,
        fn %{"ParentShards" => parent_shards, "ShardId" => shard_id}, acc ->
          maybe_start_shard(parent_shards, shard_id, state, acc)
        end
      )

    shard_map =
      child_shards
      |> Enum.group_by(&Map.fetch!(&1, "ShardId"))
      |> Map.merge(state.shard_map)

    {:reply, :ok, %__MODULE__{state | shard_pid_map: shard_pid_map, shard_map: shard_map}}
  end

  def create_table_if_not_exists(state) do
    AppState.initialize(state.app_name, state.app_state_opts)
  end

  defp describe_stream(state) do
    case get_shards(state.stream_name, state.kinesis_opts) do
      {:ok, %{"StreamStatus" => "ACTIVE", "Shards" => shards}} ->
        notify({:shards, shards}, state)

        shard_graph = build_shard_graph(shards)

        shard_map =
          Enum.reduce(shards, %{}, fn %{"ShardId" => shard_id} = s, acc ->
            Map.put(acc, shard_id, s)
          end)

        state = %{state | shard_map: shard_map}

        map = start_shards(shard_graph, state)

        {:noreply, %{state | shard_graph: shard_graph, shard_pid_map: map}}

      {:error, reason} ->
        Logger.info("[kcl_ex] error describing stream shards with result #{inspect(reason)}")

        if state.startup_attempt < @max_startup_attempts do
          sleep_period_ms = state.retry_timeout + :rand.uniform(@jitter)
          :timer.sleep(sleep_period_ms)

          state = %{state | startup_attempt: state.startup_attempt + 1}
          notify({:retrying_describe_stream, self()}, state)

          {:noreply, state, {:continue, :describe_stream}}
        else
          Logger.error(
            "[kcl_ex] error starting stream coordinator after three attempts for stream #{state.stream_name}"
          )

          {:stop, :normal, state}
        end
    end
  end

  defp build_shard_graph(shard_list) do
    graph = :digraph.new([:acyclic])

    Enum.each(shard_list, fn %{"ShardId" => shard_id} = s ->
      unless :digraph.vertex(graph, shard_id) do
        :digraph.add_vertex(graph, shard_id)
      end

      add_parent_shard(graph, s)
    end)

    graph
  end

  defp add_parent_shard(graph, %{
         "ShardId" => shard_id,
         "ParentShardId" => parent_shard_id,
         "AdjacentParentShardId" => adj_parent_shard_id
       })
       when is_binary(parent_shard_id) do
    add_vertex(graph, parent_shard_id)
    :digraph.add_edge(graph, parent_shard_id, shard_id, "parent-child")

    if is_binary(adj_parent_shard_id) do
      add_vertex(graph, adj_parent_shard_id)
      :digraph.add_edge(graph, adj_parent_shard_id, shard_id)
    end
  end

  defp add_parent_shard(_graph, _shard), do: :ok

  defp start_shards(shard_graph, %__MODULE__{} = state) do
    shard_r = list_relationships(shard_graph)

    Enum.reduce(shard_r, state.shard_pid_map, fn {shard_id, parents}, acc ->
      maybe_start_shard(parents, shard_id, state, acc)
    end)
  end

  # Shards parents are not available, so we only rely on the shard lease status
  defp maybe_start_shard([] = _parents, shard_id, state, shard_pid_map) do
    case get_lease(shard_id, state) do
      # Completed shards are not added to the shard_pid_map
      %{completed: true} ->
        shard_pid_map

      _ ->
        {:ok, shard_pid} = start_shard(shard_id, state)
        Map.put(shard_pid_map, shard_id, shard_pid)
    end
  end

  defp maybe_start_shard(parents, shard_id, state, shard_pid_map) do
    if parents_completed?(parents, shard_id, state) do
      Logger.info("Parent shards are complete so starting #{shard_id}")

      {:ok, shard_pid} = start_shard(shard_id, state)
      Map.put(shard_pid_map, shard_id, shard_pid)
    else
      shard_pid_map
    end
  end

  defp parents_completed?(parents, shard_id, state) do
    parents
    |> Enum.map(fn parent -> {parent, get_lease(parent, state)} end)
    |> Enum.all?(fn
      {_parent, %{completed: true}} ->
        true

      {parent, _} ->
        Logger.info("Parent shard #{parent} is not completed so skipping #{shard_id}")
        false
    end)
  end

  defp get_lease(shard_id, %{app_name: app_name, app_state_opts: app_state_opts}) do
    AppState.get_lease(app_name, shard_id, app_state_opts)
  end

  # Start a shard and return an updated worker map with the %{lease_ref => pid}
  @spec start_shard(
          shard_id :: String.t(),
          state :: map
        ) ::
          {:ok, pid}
  defp start_shard(
         shard_id,
         %{shard_supervisor_name: shard_supervisor, shard_args: shard_args} = state
       ) do
    shard_args =
      shard_args
      |> Keyword.put(:shard_id, shard_id)
      |> Keyword.put(
        :shard_name,
        Shard.name(state.app_name, state.stream_name, shard_id)
      )

    case Shard.start(shard_supervisor, shard_args) do
      {:ok, pid} ->
        notify({:shard_started, %{pid: pid, shard_id: shard_id}}, state)
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}
    end
  end

  ##
  # Returns a list of shape [{shard_id, [parent_shards]}]. The list is sorted from low to high on the
  # parent links list.
  defp list_relationships(graph) do
    n = graph |> :digraph.vertices() |> Enum.map(fn v -> {v, :digraph.in_neighbours(graph, v)} end)

    Enum.sort(n, fn {_, n1}, {_, n2} -> length(n1) <= length(n2) end)
  end

  defp get_shards(stream_name, kinesis_opts, shard_start_id \\ nil, shard_list \\ []) do
    describe_result =
      case shard_start_id do
        nil ->
          Kinesis.describe_stream(stream_name, kinesis_opts)

        x when is_binary(x) ->
          Kinesis.describe_stream(
            stream_name,
            Keyword.merge(kinesis_opts, exclusive_start_shard_id: x)
          )
      end

    case describe_result do
      {:ok, %{"StreamDescription" => %{"HasMoreShards" => true, "Shards" => shards}}} ->
        get_shards(
          stream_name,
          kinesis_opts,
          hd(Enum.reverse(shards))["ShardId"],
          shards ++ shard_list
        )

      {:ok, %{"StreamDescription" => %{"HasMoreShards" => false, "Shards" => shards} = stream}} ->
        {:ok, Map.put(stream, "Shards", shards ++ shard_list)}

      {:error, _} = error ->
        error

      _ ->
        {:error, :unknown}
    end
  end

  defp add_vertex(graph, shard_id) do
    case :digraph.vertex(graph, shard_id) do
      false ->
        :digraph.add_vertex(graph, shard_id)

      _ ->
        nil
    end
  end

  defp notify(message, %__MODULE__{notify_pid: notify_pid}) do
    case notify_pid do
      pid when is_pid(pid) ->
        send(pid, message)
        :ok

      nil ->
        :ok
    end
  end
end
