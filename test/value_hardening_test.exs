defmodule EKV.ValueHardeningTest do
  use ExUnit.Case, async: false

  setup do
    name = :"ekv_value_hardening_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))

    {:ok, ekv_pid} =
      EKV.start_link(
        name: name,
        data_dir: data_dir,
        shards: 1,
        cluster_size: 1,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    on_exit(fn ->
      Process.exit(ekv_pid, :shutdown)
      File.rm_rf!(data_dir)
    end)

    shard_name = EKV.Replica.shard_name(name, 0)
    %{name: name, shard_name: shard_name}
  end

  test "lookup, get, and scan surface deterministic corruption errors without creating atoms", %{
    name: name,
    shard_name: shard_name
  } do
    assert {:ok, :ok} = EKV.ValueCodec.decode(:erlang.term_to_binary(:ok))
    write_raw_value(shard_name, "warmup/read", <<131, 104>>, 1)

    for read <- [
          fn -> EKV.lookup(name, "warmup/read") end,
          fn -> EKV.get(name, "warmup/read") end,
          fn -> EKV.scan(name, "warmup/") |> Enum.to_list() end
        ] do
      assert_raise EKV.DecodeError, read
    end

    poison_name = "ekv_persisted_poison_#{System.unique_integer([:positive])}"
    poison = external_atom(poison_name)
    write_raw_value(shard_name, "poison/read", poison, 2)

    for read <- [
          fn -> EKV.lookup(name, "poison/read") end,
          fn -> EKV.get(name, "poison/read") end,
          fn -> EKV.scan(name, "poison/") |> Enum.to_list() end
        ] do
      error = assert_raise EKV.DecodeError, read
      assert error.reason == :invalid_or_unsafe_external_term
    end

    assert_atom_not_created(poison_name)
  end

  test "replication and snapshot recovery reject poisoned values without crashing", %{
    name: name,
    shard_name: shard_name
  } do
    poison_name = "ekv_replication_poison_#{System.unique_integer([:positive])}"
    poison = external_atom(poison_name)
    now = System.system_time(:nanosecond)

    send(
      shard_name,
      {:ekv_replication_batch, node(), 0, "remote-origin",
       [
         {"poison/live", poison, now, 1, nil, nil},
         {"poison/after", :erlang.term_to_binary("must-not-advance"), now + 1, 2, nil, nil}
       ]}
    )

    full_request_id = expect_full_sync(shard_name, :remote@host)

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, full_request_id, :full,
       [{"poison/snapshot", poison, now + 1, "remote-origin", 2, nil, nil}],
       %{"remote-origin" => 2}}
    )

    delta_request_id = expect_delta_sync(shard_name, :remote@host, "remote-origin", 2)

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, delta_request_id, :delta,
       [{"poison/log", poison, now + 2, "remote-origin", 3, nil, nil}], %{"remote-origin" => 3}}
    )

    %{db: db} = :sys.get_state(shard_name)

    assert Process.alive?(Process.whereis(shard_name))
    assert EKV.get(name, "poison/live") == nil
    assert EKV.get(name, "poison/after") == nil
    assert EKV.get(name, "poison/snapshot") == nil
    assert EKV.get(name, "poison/log") == nil
    refute Map.has_key?(EKV.Store.local_progress_summary(db), "remote-origin")
    assert_atom_not_created(poison_name)
  end

  test "CAS accept rejects poisoned values with a nack", %{shard_name: shard_name} do
    poison_name = "ekv_cas_poison_#{System.unique_integer([:positive])}"
    poison = external_atom(poison_name)
    ref = make_ref()
    key = "poison/cas"
    now = System.system_time(:nanosecond)
    entry = {key, poison, now, "remote-origin", nil, nil}
    register_test_voter(shard_name, self(), "remote")

    send(shard_name, {:ekv_accept, ref, self(), key, 1, "remote", entry, 0})

    assert_receive {:ekv, 2, :accept_nack, {^ref, _pid, _node_id}, %{}}, 1_000
    assert Process.alive?(Process.whereis(shard_name))
    assert_atom_not_created(poison_name)
  end

  test "CAS reads fail conservatively on a corrupted persisted value", %{
    name: name,
    shard_name: shard_name
  } do
    key = "poison/cas-read"
    poison_name = "ekv_cas_read_poison_#{System.unique_integer([:positive])}"
    poison = external_atom(poison_name)
    write_raw_value(shard_name, key, poison, 1)

    assert {:error, :corrupt_value} = EKV.put(name, key, "replacement", if_vsn: nil)
    assert Process.alive?(Process.whereis(shard_name))
    assert_atom_not_created(poison_name)
  end

  test "CAS commits cannot promote a corrupted accepted value", %{
    name: name,
    shard_name: shard_name
  } do
    key = "poison/slim-cas-commit"
    poison_name = "ekv_slim_commit_poison_#{System.unique_integer([:positive])}"
    poison = external_atom(poison_name)
    now = System.system_time(:nanosecond)
    %{db: db} = :sys.get_state(shard_name)
    register_test_voter(shard_name, self(), "remote-ballot")

    assert {:ok, true} =
             EKV.Store.paxos_accept(db, key, 1, "remote-ballot", [
               poison,
               now,
               "remote-ballot",
               nil,
               nil
             ])

    send(
      shard_name,
      {:ekv_cas_committed, self(), key, 1, "remote-ballot", nil, 0, "remote-ballot", 1}
    )

    :sys.get_state(shard_name)

    assert EKV.get(name, key) == nil
    refute Map.has_key?(EKV.Store.local_progress_summary(db), "remote-ballot")
    assert Process.alive?(Process.whereis(shard_name))
    assert_atom_not_created(poison_name)

    payload_key = "poison/payload-cas-commit"

    assert {:ok, true} =
             EKV.Store.paxos_accept(db, payload_key, 2, "remote-ballot", [
               poison,
               now,
               "remote-ballot",
               nil,
               nil
             ])

    safe_payload =
      {payload_key, :erlang.term_to_binary("safe-looking-payload"), now, "remote-ballot", nil,
       nil}

    send(
      shard_name,
      {:ekv_cas_committed, self(), payload_key, 2, "remote-ballot", safe_payload, 0,
       "remote-ballot", 2}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, payload_key) == nil
    assert Process.alive?(Process.whereis(shard_name))
    assert_atom_not_created(poison_name)
  end

  test "replication batches reject the hard entry-count overflow atomically", %{
    name: name,
    shard_name: shard_name
  } do
    now = System.system_time(:nanosecond)
    value_binary = :erlang.term_to_binary("safe")
    register_test_voter(shard_name, self(), "remote-ballot")

    entries =
      for seq <- 1..(EKV.WireEnvelope.max_batch_entries() + 1) do
        {"overflow/#{seq}", value_binary, now + seq, seq, nil, nil}
      end

    send(shard_name, {:ekv_replication_batch, node(), 0, "overflow-origin", entries})
    :sys.get_state(shard_name)

    assert EKV.get(name, "overflow/1") == nil
    assert EKV.get(name, "overflow/#{EKV.WireEnvelope.max_batch_entries() + 1}") == nil
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "delta sync requires an inflight contiguous request and exact terminal progress", %{
    name: name,
    shard_name: shard_name
  } do
    now = System.system_time(:nanosecond)
    origin = "gap-origin"
    value_binary = :erlang.term_to_binary("safe")

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, :delta,
       [{"delta/unsolicited", value_binary, now, origin, 10, nil, nil}], %{origin => 10}}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, "delta/unsolicited") == nil

    request_id = expect_delta_sync(shard_name, :remote@host, origin, 0)

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, request_id, :delta,
       [{"delta/ahead", value_binary, now + 1, origin, 1, nil, nil}], %{origin => 100}}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, "delta/ahead") == nil

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, request_id, :delta,
       [{"delta/wrong-origin", value_binary, now + 2, origin, 1, nil, nil}],
       %{"other-origin" => 1}}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, "delta/wrong-origin") == nil

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, request_id, :delta,
       [{"delta/valid", value_binary, now + 3, origin, 1, nil, nil}], %{origin => 1}}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, "delta/valid") == "safe"

    %{db: db} = :sys.get_state(shard_name)
    assert EKV.Store.local_progress_summary(db)[origin] == 1
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "replication batches reject hard aggregate value-byte overflow atomically", %{
    name: name,
    shard_name: shard_name
  } do
    now = System.system_time(:nanosecond)
    value_binary = :erlang.term_to_binary(String.duplicate("x", 4_200_000))

    entries = [
      {"aggregate/1", value_binary, now, 1, nil, nil},
      {"aggregate/2", value_binary, now + 1, 2, nil, nil}
    ]

    send(shard_name, {:ekv_replication_batch, node(), 0, "aggregate-origin", entries})
    :sys.get_state(shard_name)

    assert EKV.get(name, "aggregate/1") == nil
    assert EKV.get(name, "aggregate/2") == nil
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "oversized keys are rejected at API and replication boundaries", %{
    name: name,
    shard_name: shard_name
  } do
    oversized_key = :binary.copy("k", EKV.WireEnvelope.max_key_bytes() + 1)

    assert_raise ArgumentError, ~r/EKV key must be a binary/, fn ->
      EKV.put(name, oversized_key, "value")
    end

    send(
      shard_name,
      {:ekv_replication_batch, node(), 0, "oversized-key-origin",
       [{oversized_key, nil, System.system_time(:nanosecond), 1, nil, 1}]}
    )

    %{db: db} = :sys.get_state(shard_name)
    assert EKV.Store.get(db, oversized_key) == nil
    refute Map.has_key?(EKV.Store.local_progress_summary(db), "oversized-key-origin")
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "non-contiguous replication batches never fall back to partial writes", %{
    name: name,
    shard_name: shard_name
  } do
    now = System.system_time(:nanosecond)
    value_binary = :erlang.term_to_binary("safe")

    send(
      shard_name,
      {:ekv_replication_batch, node(), 0, "sequence-origin",
       [
         {"sequence/1", value_binary, now, 1, nil, nil},
         {"sequence/3", value_binary, now + 1, 3, nil, nil}
       ]}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, "sequence/1") == nil
    assert EKV.get(name, "sequence/3") == nil
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "malformed peer origins are rejected without crashing the replica", %{
    name: name,
    shard_name: shard_name
  } do
    now = System.system_time(:nanosecond)
    value_binary = :erlang.term_to_binary("safe")

    send(
      shard_name,
      {:ekv_replication_batch, node(), 0, {:malformed, :origin},
       [{"origin/replication", value_binary, now, 1, nil, nil}]}
    )

    request_id = expect_full_sync(shard_name, :remote@host)

    send(
      shard_name,
      {:ekv_sync, :remote@host, 0, request_id, :full,
       [{"origin/sync", value_binary, now, {:malformed, :origin}, 1, nil, nil}],
       %{"remote-origin" => 1}}
    )

    send(
      shard_name,
      {:ekv_cas_committed, self(), "origin/commit", 1, "remote-ballot", nil, 0,
       {:malformed, :origin}, 1}
    )

    :sys.get_state(shard_name)
    assert EKV.get(name, "origin/replication") == nil
    assert EKV.get(name, "origin/sync") == nil
    assert EKV.get(name, "origin/commit") == nil
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "CAS accept normalizes atom origins before the NIF boundary", %{shard_name: shard_name} do
    ref = make_ref()
    key = "origin/cas-accept"

    entry =
      {key, :erlang.term_to_binary("safe"), System.system_time(:nanosecond), node(), nil, nil}

    register_test_voter(shard_name, self(), "remote")
    send(shard_name, {:ekv_accept, ref, self(), key, 1, "remote", entry, 0})

    assert_receive {:ekv, 2, :accepted, {^ref, _pid, _node_id}, %{}}, 1_000
    assert Process.alive?(Process.whereis(shard_name))
  end

  test "corrupt previous values do not crash subscription delete dispatch", %{
    name: name,
    shard_name: shard_name
  } do
    key = "poison/previous"
    poison_name = "ekv_previous_poison_#{System.unique_integer([:positive])}"
    poison = external_atom(poison_name)
    write_raw_value(shard_name, key, poison, 1)
    :ok = EKV.subscribe(name, key)
    Process.sleep(25)

    send(
      shard_name,
      {:ekv_replication_batch, node(), 0, "remote-origin", [{key, nil, 2, 1, nil, 2}]}
    )

    :sys.get_state(shard_name)

    assert_receive {:ekv, [%EKV.Event{type: :delete, key: ^key, value: nil}], %{name: ^name}},
                   1_000

    assert Process.alive?(Process.whereis(shard_name))
    assert_atom_not_created(poison_name)
  end

  defp write_raw_value(shard_name, key, value_binary, timestamp) do
    %{db: db, stmts: stmts} = :sys.get_state(shard_name)

    assert {:ok, true, _origin_seq, _local_progress_seq} =
             EKV.Store.write_entry(
               db,
               stmts.kv_upsert,
               stmts.keyref_upsert,
               stmts.oplog_insert,
               key,
               value_binary,
               timestamp,
               "raw-test-origin",
               nil
             )
  end

  defp expect_delta_sync(shard_name, source_node, origin_node, from_seq) do
    request_id = make_ref()

    :sys.replace_state(shard_name, fn state ->
      %{
        state
        | sync_requests:
            Map.put(state.sync_requests, source_node, %{
              request: {:delta, origin_node, from_seq},
              id: request_id
            })
      }
    end)

    request_id
  end

  defp expect_full_sync(shard_name, source_node) do
    request_id = make_ref()

    :sys.replace_state(shard_name, fn state ->
      %{
        state
        | sync_requests:
            Map.put(state.sync_requests, source_node, %{request: :full, id: request_id})
      }
    end)

    request_id
  end

  defp register_test_voter(shard_name, voter_pid, node_id) do
    voter_node = node(voter_pid)

    :sys.replace_state(shard_name, fn state ->
      %{
        state
        | remote_shards: Map.put(state.remote_shards, voter_node, voter_pid),
          member_node_ids: Map.put(state.member_node_ids, voter_node, node_id),
          remote_features: Map.put(state.remote_features, voter_node, MapSet.new())
      }
    end)
  end

  defp external_atom(name) when byte_size(name) < 256 do
    <<131, 119, byte_size(name), name::binary>>
  end

  defp assert_atom_not_created(name) do
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
  end
end
