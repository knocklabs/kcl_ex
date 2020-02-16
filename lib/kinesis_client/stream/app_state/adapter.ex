defmodule KinesisClient.Stream.AppState.Adapter do
  @moduledoc false

  alias KinesisClient.Stream.AppState.ShardLease

  @callback get_lease(app_name :: String.t(), shard_id :: String.t()) ::
              ShardLease.t() | :not_found | {:error, any}

  @callback get_lease(app_name :: String.t(), shard_id :: String.t(), opts :: keyword) ::
              ShardLease.t() | :not_found | {:error, any}

  @callback create_lease(app_name :: String.t(), shard_id :: String.t(), lease_owner :: String.t()) ::
              :ok | {:error, any}

  @callback create_lease(
              app_name :: String.t(),
              shard_id :: String.t(),
              lease_owner :: String.t(),
              opts :: keyword
            ) ::
              :ok | :already_exists | {:error, any}

  @callback update_lease_owner(shard_id :: String.t(), lease_owner :: String.t()) ::
              :ok | {:error, any}

  @callback update_lease_owner(shard_id :: String.t(), lease_owner :: String.t(), opts :: keyword) ::
              :ok | {:error, any}

  @callback update_checkpoint(
              shard_id :: String.t(),
              lease_owner :: String.t(),
              checkpoint :: integer
            ) ::
              :ok | {:error, any}

  @callback update_checkpoint(
              shard_id :: String.t(),
              lease_owner :: String.t(),
              checkpoint :: integer,
              opts :: keyword
            ) ::
              :ok | {:error, any}

  @callback renew_lease(
              app_name :: String.t(),
              shard_lease :: ShardLease.t()
            ) ::
              {:ok, new_lease_count :: integer} | :lease_renew_failed | {:error, any}

  @callback renew_lease(app_name :: String.t(), shard_lease :: ShardLease.t(), opts :: keyword) ::
              {:ok, new_lease_count :: integer} | :lease_renew_failed | {:error, any}

  @callback take_lease(
              app_name :: String.t(),
              shard_id :: String.t(),
              new_owner :: String.t(),
              lease_count :: integer
            ) ::
              {:ok, new_lease_count :: integer} | :lease_take_failed | {:error, any}

  @callback take_lease(
              app_name :: String.t(),
              shard_id :: String.t(),
              new_owner :: String.t(),
              lease_count :: integer,
              opts :: keyword
            ) ::
              {:ok, new_lease_count :: integer} | :lease_take_failed | {:error, any}

  @callback close_shard(shard_id :: String.t()) :: :ok | {:error, any}

  @callback close_shard(shard_id :: String.t(), opts :: keyword) :: :ok | {:error, any}
end
