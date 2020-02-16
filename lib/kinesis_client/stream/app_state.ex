defmodule KinesisClient.Stream.AppState do
  @moduledoc """
  The AppState is where the information about Stream shards are stored. ShardConsumers will
  checkpoint the records, and the `KinesisClient.Stream.Coordinator` will check here to determine
  what shards to consume.
  """
  alias KinesisClient.Stream.AppState.Adapter

  @behaviour Adapter

  @doc """
  Get a `KinesisClient.Stream.AppState.ShardInfo` struct by shard_id. If there is not an existing
  record, returns `:not_found`.
  """
  @impl Adapter
  def get_lease(app_name, shard_id, opts \\ []),
    do: adapter(opts).get_lease(app_name, shard_id, opts)

  @doc """
  Persists a new ShardInfo record. Returns an error if there is already a record for that `shard_id`
  """
  @impl Adapter
  def create_lease(app_name, shard_id, lease_owner, opts \\ []),
    do: adapter(opts).create_lease(app_name, shard_id, lease_owner, opts)

  @doc """
  Set a shards lease owner with the unique reference that the ShardConsumer will have. This will be
  checked on calls to `update_checkpoint/4`
  """
  @impl Adapter
  def update_lease_owner(shard_id, lease, opts \\ []),
    do: adapter(opts).update_lease_owner(shard_id, lease, opts)

  @doc """
  Update the checkpoint of the shard with the last sequence number that was processed by a
  ShardConsumer. Will return {:error, :lead_invalid} if the `lease` does not match what is in
  `ShardInfo` and the checkpoint will not be updated.
  """
  @impl Adapter
  def update_checkpoint(shard_id, lease, checkpoint, opts \\ []),
    do: adapter(opts).update_checkpoint(shard_id, lease, checkpoint, opts)

  @doc """
  Renew lease. Increments :lease_count.
  """
  @impl Adapter
  def renew_lease(app_name, shard_lease, opts \\ []),
    do: adapter(opts).renew_lease(app_name, shard_lease, opts)

  @impl Adapter
  def take_lease(app_name, shard_id, new_owner, lease_count, opts \\ []),
    do: adapter(opts).take_lease(app_name, shard_id, new_owner, lease_count, opts)

  @doc """
  Marks a shard as CLOSED. This indicates that all records have been processed by the app.
  """
  @impl Adapter
  def close_shard(shard_id, opts \\ []), do: adapter(opts).close_shard(shard_id, opts)

  defp adapter(opts) do
    Keyword.get(opts, :adapter, KinesisClient.Stream.AppState.Dynamo)
  end
end
