defmodule KinesisClient.Stream.AppState.ShardLeaseTest do
  use KinesisClient.Case

  alias ExAws.Dynamo
  alias KinesisClient.Stream.AppState.ShardLease

  test "ExAws.Dynamo.Encodable.encode/2 implemented" do
    result = Dynamo.Encoder.encode(%ShardLease{}, [])

    assert result == %{
             "M" => %{
               "checkpoint" => %{"NULL" => true},
               "completed" => %{"BOOL" => false},
               "lease_count" => %{"NULL" => true},
               "lease_owner" => %{"NULL" => true},
               "shard_id" => %{"NULL" => true}
             }
           }
  end

  test "ExAws.Dynamo.Encodable.decode/2 implemented" do
    assert %ShardLease{} = Dynamo.Decoder.decode(%{
             "M" => %{
               "checkpoint" => %{"NULL" => "true"},
               "completed" => %{"BOOL" => "false"},
               "lease_count" => %{"NULL" => "true"},
               "lease_owner" => %{"NULL" => "true"},
               "shard_id" => %{"NULL" => "true"}
             }
    }, as: ShardLease)
  end
end
