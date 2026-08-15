defmodule EKV.CASWireHardeningTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias EKV.TestCluster

  @max_signed_64 9_223_372_036_854_775_807
  @local_node_id "local-voter"
  @remote_node_id "remote-voter"
  setup do
    [{peer, remote_node}] = peers = TestCluster.start_peers(1)
    sender = TestCluster.start_wire_relay(remote_node, self())
    name = :"cas_wire_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))
    File.rm_rf!(data_dir)

    {:ok, supervisor} =
      EKV.start_link(
        name: name,
        data_dir: data_dir,
        shards: 1,
        cluster_size: 2,
        node_id: @local_node_id,
        log: false,
        gc_interval: :timer.hours(1),
        tombstone_ttl: :timer.hours(24 * 7)
      )

    Process.unlink(supervisor)
    shard = EKV.Replica.shard_name(name, 0)

    :sys.replace_state(shard, fn state ->
      %{
        state
        | remote_shards: %{remote_node => sender},
          member_node_ids: %{remote_node => @remote_node_id},
          remote_features: %{remote_node => MapSet.new()}
      }
    end)

    on_exit(fn ->
      send(sender, :stop)

      if Process.alive?(supervisor) do
        Supervisor.stop(supervisor, :shutdown, 5_000)
      end

      TestCluster.stop_peers(peers)
      File.rm_rf!(data_dir)
    end)

    %{name: name, shard: shard, sender: sender, remote_node: remote_node, peer: peer}
  end

  test "prepare and accept reject malformed or unknown proposers before durable mutation", %{
    name: name,
    shard: shard,
    sender: sender
  } do
    key = "cas-wire/acceptor"
    ballot = System.system_time(:nanosecond)

    send(shard, {:ekv_prepare, make_ref(), :not_a_pid, key, ballot, @remote_node_id, 0})
    send(shard, {:ekv_prepare, make_ref(), self(), key, ballot, @remote_node_id, 0})
    emit(sender, shard, {:ekv_prepare, :not_a_ref, sender, key, ballot, @remote_node_id, 0})
    emit(sender, shard, {:ekv_prepare, make_ref(), sender, key, ballot, @remote_node_id, 1})
    emit(sender, shard, {:ekv_prepare, make_ref(), sender, key, 0, @remote_node_id, 0})

    emit(
      sender,
      shard,
      {:ekv_prepare, make_ref(), sender, key, @max_signed_64, @remote_node_id, 0}
    )

    emit(
      sender,
      shard,
      {:ekv_prepare, make_ref(), sender, key, ballot, @remote_node_id <> <<0>>, 0}
    )

    valid_ref = make_ref()
    emit(sender, shard, {:ekv_prepare, valid_ref, sender, key, ballot, @remote_node_id, 0})

    assert_receive {:wire_reply, ^sender,
                    {:ekv, 2, :promise, {^valid_ref, replica_pid, @local_node_id, 0, "", nil},
                     %{}}},
                   2_000

    assert is_pid(replica_pid)

    value_binary = :erlang.term_to_binary("accepted")
    entry = {key, value_binary, ballot, @remote_node_id, nil, nil}
    invalid_accept_ref = make_ref()

    emit(
      sender,
      shard,
      {:ekv_accept, invalid_accept_ref, sender, key, ballot + 1_000, @remote_node_id <> <<0>>,
       entry, 0}
    )

    valid_accept_ref = make_ref()

    emit(
      sender,
      shard,
      {:ekv_accept, valid_accept_ref, sender, key, ballot, @remote_node_id, entry, 0}
    )

    assert_receive {:wire_reply, ^sender,
                    {:ekv, 2, :accepted, {^valid_accept_ref, ^replica_pid, @local_node_id}, %{}}},
                   2_000

    assert Process.alive?(replica_pid)
    assert EKV.get(name, key) == nil
  end

  test "promise and accepted responses cannot forge quorum identities", %{
    name: name,
    shard: shard,
    sender: sender
  } do
    key = "cas-wire/quorum"
    task = Task.async(fn -> EKV.put(name, key, "committed", if_vsn: nil, timeout: 5_000) end)
    {ref, op} = pending_cas(shard)

    send(shard, {:ekv_promise, ref, self(), @remote_node_id, 0, "", nil})
    emit(sender, shard, {:ekv_promise, ref, sender, "forged-id", 0, "", nil})
    emit(sender, shard, {:ekv_promise, ref, sender, @remote_node_id, 0, nil, nil})

    malformed_row = [:erlang.term_to_binary("bad"), @max_signed_64, @remote_node_id, nil, nil]
    emit(sender, shard, {:ekv_promise, ref, sender, @remote_node_id, 0, "", malformed_row})

    assert_eventually(fn ->
      current = :sys.get_state(shard).pending_cas[ref]
      current.phase == :prepare and current.promises == op.promises
    end)

    emit(sender, shard, {:ekv_promise, ref, sender, @remote_node_id, 0, "", nil})

    assert_eventually(fn -> :sys.get_state(shard).pending_cas[ref].phase == :accept end)

    send(shard, {:ekv_accepted, ref, self(), @remote_node_id})
    emit(sender, shard, {:ekv_accepted, ref, sender, "forged-id"})

    assert_eventually(fn ->
      current = :sys.get_state(shard).pending_cas[ref]
      current.phase == :accept and current.accepts == MapSet.new([@local_node_id])
    end)

    emit(sender, shard, {:ekv_accepted, ref, sender, @remote_node_id})

    assert match?({:ok, _version}, Task.await(task, 5_000))
    assert EKV.get(name, key) == "committed"
    assert Process.alive?(Process.whereis(shard))
  end

  test "commit requires a connected proposer and safe ballot and sequence bounds", %{
    name: name,
    shard: shard,
    sender: sender
  } do
    key = "cas-wire/commit"
    ballot = System.system_time(:nanosecond)
    value_binary = :erlang.term_to_binary("committed")
    entry = {key, value_binary, ballot, @remote_node_id, nil, nil}

    send(
      shard,
      {:ekv_cas_committed, key, ballot, @remote_node_id, entry, 0, @remote_node_id, 1}
    )

    send(
      shard,
      {:ekv_cas_committed, self(), key, ballot, @remote_node_id, entry, 0, @remote_node_id, 1}
    )

    emit(
      sender,
      shard,
      {:ekv_cas_committed, sender, key, ballot, "forged-id", entry, 0, @remote_node_id, 1}
    )

    emit(
      sender,
      shard,
      {:ekv_cas_committed, sender, key, ballot, @remote_node_id, entry, 0, @remote_node_id,
       @max_signed_64}
    )

    assert_eventually(fn ->
      state = :sys.get_state(shard)
      EKV.get(name, key) == nil and Map.get(state.local_progress, @remote_node_id, 0) == 0
    end)

    emit(
      sender,
      shard,
      {:ekv_cas_committed, sender, key, ballot, @remote_node_id, entry, 0, @remote_node_id, 1}
    )

    assert_eventually(fn ->
      state = :sys.get_state(shard)
      EKV.get(name, key) == "committed" and Map.get(state.local_progress, @remote_node_id) == 1
    end)

    assert Process.alive?(Process.whereis(shard))
  end

  test "a valid higher nack advances the next local proposal immediately", %{
    name: name,
    shard: shard,
    sender: sender
  } do
    first =
      Task.async(fn ->
        EKV.put(name, "cas-wire/nack-1", "one", if_vsn: nil, timeout: 5_000)
      end)

    {first_ref, first_op} = pending_cas(shard)
    promised_counter = elem(first_op.ballot, 0) + 1_000_000

    emit(
      sender,
      shard,
      {:ekv_nack, first_ref, sender, @remote_node_id, promised_counter, @remote_node_id}
    )

    assert {:error, :conflict} = Task.await(first, 5_000)
    assert :sys.get_state(shard).ballot_counter >= promised_counter

    second =
      Task.async(fn ->
        EKV.put(name, "cas-wire/nack-2", "two", if_vsn: nil, timeout: 5_000)
      end)

    {second_ref, second_op} = pending_cas(shard)
    {second_counter, _node_id} = second_op.ballot
    assert second_counter > promised_counter

    emit(sender, shard, {:ekv_promise, second_ref, sender, @remote_node_id, 0, "", nil})
    assert_eventually(fn -> :sys.get_state(shard).pending_cas[second_ref].phase == :accept end)
    emit(sender, shard, {:ekv_accepted, second_ref, sender, @remote_node_id})

    assert match?({:ok, _version}, Task.await(second, 5_000))
    assert EKV.get(name, "cas-wire/nack-2") == "two"
  end

  test "a higher local prepare nack advances the next proposal immediately", %{
    name: name,
    shard: shard,
    sender: sender
  } do
    key = "cas-wire/local-nack"
    promised_counter = System.system_time(:nanosecond) + :timer.minutes(2) * 1_000_000
    %{db: db} = :sys.get_state(shard)

    assert {:ok, :promise, 0, "", nil} =
             EKV.Store.paxos_prepare(db, key, promised_counter, "seed-voter")

    first =
      Task.async(fn ->
        EKV.put(name, key, "one", if_vsn: nil, timeout: 5_000)
      end)

    assert {:error, :conflict} = Task.await(first, 5_000)
    assert :sys.get_state(shard).ballot_counter >= promised_counter

    second =
      Task.async(fn ->
        EKV.put(name, key, "two", if_vsn: nil, timeout: 5_000)
      end)

    {second_ref, second_op} = pending_cas(shard)
    {second_counter, _node_id} = second_op.ballot
    assert second_counter > promised_counter

    emit(sender, shard, {:ekv_promise, second_ref, sender, @remote_node_id, 0, "", nil})
    assert_eventually(fn -> :sys.get_state(shard).pending_cas[second_ref].phase == :accept end)
    emit(sender, shard, {:ekv_accepted, second_ref, sender, @remote_node_id})

    assert match?({:ok, _version}, Task.await(second, 5_000))
    assert EKV.get(name, key) == "two"
  end

  test "a far-future nack cannot poison the local ballot counter", %{
    name: name,
    shard: shard,
    sender: sender
  } do
    task =
      Task.async(fn ->
        EKV.put(name, "cas-wire/future-nack", "safe", if_vsn: nil, timeout: 5_000)
      end)

    {ref, op} = pending_cas(shard)
    counter_before = :sys.get_state(shard).ballot_counter
    far_future = System.system_time(:nanosecond) + :timer.minutes(6) * 1_000_000

    emit(
      sender,
      shard,
      {:ekv_nack, ref, sender, @remote_node_id, far_future, @remote_node_id}
    )

    assert :sys.get_state(shard).ballot_counter == counter_before
    assert :sys.get_state(shard).pending_cas[ref].responded == op.responded

    emit(sender, shard, {:ekv_promise, ref, sender, @remote_node_id, 0, "", nil})
    assert_eventually(fn -> :sys.get_state(shard).pending_cas[ref].phase == :accept end)
    emit(sender, shard, {:ekv_accepted, ref, sender, @remote_node_id})

    assert match?({:ok, _version}, Task.await(task, 5_000))
    assert EKV.get(name, "cas-wire/future-nack") == "safe"
  end

  defp emit(sender, target, message) do
    ack_ref = make_ref()
    target = if is_atom(target), do: Process.whereis(target), else: target
    send(sender, {:emit, target, message, ack_ref})
    assert_receive {:wire_emitted, ^ack_ref}, 2_000
    :ok
  end

  defp pending_cas(shard) do
    assert_eventually(fn -> map_size(:sys.get_state(shard).pending_cas) == 1 end)
    [{ref, op}] = Map.to_list(:sys.get_state(shard).pending_cas)
    {ref, op}
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
