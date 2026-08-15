defmodule EKV.MemberIngressHardeningTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias EKV.{Replica, TestCluster, WireEnvelope, WireProtocol}

  setup do
    [{_peer, remote_node}] = peers = TestCluster.start_peers(1)
    relay = TestCluster.start_wire_relay(remote_node, self())
    name = :"member_ingress_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))
    File.rm_rf!(data_dir)

    {:ok, supervisor} =
      EKV.start_link(
        name: name,
        data_dir: data_dir,
        shards: 1,
        cluster_size: 2,
        node_id: "local-voter",
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    Process.unlink(supervisor)
    shard = Replica.shard_name(name, 0)

    on_exit(fn ->
      send(relay, :stop)

      if Process.alive?(supervisor) do
        Supervisor.stop(supervisor, :shutdown, 5_000)
      end

      TestCluster.stop_peers(peers)
      File.rm_rf!(data_dir)
    end)

    %{name: name, shard: shard, relay: relay, remote_node: remote_node}
  end

  test "malformed handshake pid, identity, progress, shard metadata, and features are inert", %{
    shard: shard,
    relay: relay
  } do
    oversized_id = :binary.copy("n", WireEnvelope.max_origin_bytes() + 1)
    forged_map_set = %{__struct__: MapSet, map: :not_a_map, version: 2}

    malformed = [
      {:ekv_member_connect, :not_a_pid, 0, 1, %{}, "remote-voter", MapSet.new()},
      {:ekv_member_connect, relay, -1, 1, %{}, "remote-voter", MapSet.new()},
      {:ekv_member_connect, relay, 0, 0, %{}, "remote-voter", MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, %{}, "", MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, %{}, oversized_id, MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, %{}, "remote" <> <<0>>, MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, %{"origin" => -1}, "remote-voter", MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, forged_map_set, "remote-voter", MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, [], "remote-voter", MapSet.new()},
      {:ekv_member_connect, relay, 0, 1, %{}, "remote-voter", MapSet.new([:unknown])},
      {:ekv_member_connect, relay, 0, 1, %{}, "remote-voter", %{wire_compression: :yes}},
      {:ekv_member_connect, relay, 0, 1, %{}, "remote-voter", forged_map_set}
    ]

    Enum.each(malformed, &send(shard, &1))

    assert {:ok, invalid_feature_message} =
             WireProtocol.encode(
               {:ekv_member_connect, relay, 0, 1, %{}, "remote-voter"},
               %{features: %{wire_compression: :yes}}
             )

    send(shard, invalid_feature_message)

    send(
      shard,
      {:ekv, WireProtocol.version(), :member_connect, {relay, 0, 1, %{}, "remote-voter"},
       %{features: forged_map_set}}
    )

    send(
      shard,
      {:ekv, WireProtocol.version(), :member_connect,
       {relay, 0, 1, forged_map_set, "remote-voter"}, %{features: %{}}}
    )

    state = :sys.get_state(shard)
    assert state.remote_shards == %{}
    assert state.member_node_ids == %{}
    assert state.remote_features == %{}
    assert Process.alive?(Process.whereis(shard))
  end

  test "malformed or forged progress acknowledgements cannot mutate peer progress", %{
    shard: shard,
    relay: relay,
    remote_node: remote_node
  } do
    :sys.replace_state(shard, fn state ->
      %{
        state
        | remote_shards: %{remote_node => relay},
          member_node_ids: %{remote_node => "remote-voter"},
          remote_member_progress: %{remote_node => %{"remote-voter" => 3}}
      }
    end)

    malformed = [
      {:ekv_progress_ack, :not_a_pid, 0, :full, %{}},
      {:ekv_progress_ack, relay, 1, :full, %{}},
      {:ekv_progress_ack, relay, 0, :unknown, %{}},
      {:ekv_progress_ack, relay, 0, :delta, []},
      {:ekv_progress_ack, relay, 0, :delta,
       %{
         __struct__: MapSet,
         map: :not_a_map,
         version: 2
       }},
      {:ekv_progress_ack, relay, 0, :delta, %{"remote-voter" => -1}},
      {:ekv_progress_ack, self(), 0, :full, %{"remote-voter" => 99}}
    ]

    Enum.each(malformed, &send(shard, &1))
    state = :sys.get_state(shard)

    assert state.remote_member_progress[remote_node] == %{"remote-voter" => 3}
    assert Process.alive?(Process.whereis(shard))
  end

  test "full sync is bound to the exact active request and stale terminal progress is rejected",
       %{
         name: name,
         shard: shard,
         relay: relay,
         remote_node: remote_node
       } do
    request_id = make_ref()
    stale_request_id = make_ref()
    key = "full-sync/exact-request"
    value = :erlang.term_to_binary("accepted")
    entry = {key, value, 10, "remote-voter", 1, nil, nil}

    :sys.replace_state(shard, fn state ->
      %{
        state
        | remote_shards: %{remote_node => relay},
          member_node_ids: %{remote_node => "remote-voter"},
          sync_inflight: %{remote_node => System.monotonic_time(:millisecond)},
          sync_requests: %{remote_node => %{request: :full, id: request_id}},
          full_sync_inflight: remote_node
      }
    end)

    send(
      shard,
      {:ekv_sync, remote_node, 0, stale_request_id, :full, [entry], %{"remote-voter" => 1}}
    )

    state = :sys.get_state(shard)
    assert EKV.get(name, key) == nil
    assert state.sync_requests[remote_node] == %{request: :full, id: request_id}
    assert Map.get(state.local_progress, "remote-voter", 0) == 0

    send(shard, {:ekv_sync, remote_node, 0, request_id, :full, [entry], %{"remote-voter" => 1}})

    assert_eventually(fn -> EKV.get(name, key) == "accepted" end)
    state = :sys.get_state(shard)
    refute Map.has_key?(state.sync_requests, remote_node)
    assert state.local_progress["remote-voter"] == 1
  end

  test "wire protocol v1 member traffic fails closed", %{
    name: name,
    shard: shard,
    relay: relay,
    remote_node: remote_node
  } do
    key = "protocol-v1/rejected"
    ballot = System.system_time(:nanosecond)
    entry = {key, :erlang.term_to_binary("old"), ballot, "remote-voter", nil, nil}

    :sys.replace_state(shard, fn state ->
      %{
        state
        | remote_shards: %{remote_node => relay},
          member_node_ids: %{remote_node => "remote-voter"},
          remote_features: %{remote_node => MapSet.new()}
      }
    end)

    send(
      shard,
      {:ekv, 1, :cas_committed, {relay, key, ballot, "remote-voter", entry, 0, "remote-voter", 1},
       %{}}
    )

    state = :sys.get_state(shard)
    assert EKV.get(name, key) == nil
    assert Map.get(state.local_progress, "remote-voter", 0) == 0
    assert Process.alive?(Process.whereis(shard))
  end

  defp assert_eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true before timeout")
      end

      Process.sleep(10)
      do_assert_eventually(fun, deadline)
    end
  end
end
