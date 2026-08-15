defmodule EKV.WireEnvelopeTest do
  use ExUnit.Case, async: true

  alias EKV.{ValueCodec, WireEnvelope, WireProtocol}

  @max_int64 9_223_372_036_854_775_807

  test "exposes canonical collection, message, key, origin, and payload limits" do
    assert WireEnvelope.max_batch_entries() == 4096
    assert WireEnvelope.max_batch_bytes() == 8 * 1024 * 1024
    assert WireEnvelope.max_payload_bytes() < WireEnvelope.max_batch_bytes()
    assert WireEnvelope.max_key_bytes() == 64 * 1024
    assert WireEnvelope.max_origin_bytes() == 1024
    assert WireEnvelope.max_encoded_value_bytes() < WireEnvelope.max_payload_bytes()
  end

  test "normalizes bounded binary, atom, and integer origins" do
    assert {:ok, "node@host"} = WireEnvelope.normalize_origin("node@host")
    assert {:ok, "node@host"} = WireEnvelope.normalize_origin(:node@host)
    assert {:ok, "-42"} = WireEnvelope.normalize_origin(-42)

    max_origin = :binary.copy("o", WireEnvelope.max_origin_bytes())
    assert {:ok, ^max_origin} = WireEnvelope.normalize_origin(max_origin)
    assert {:error, :empty_origin} = WireEnvelope.normalize_origin("")
    assert {:error, :origin_too_large} = WireEnvelope.normalize_origin(max_origin <> "o")
    assert {:error, :invalid_origin} = WireEnvelope.normalize_origin({:node, "host"})
    assert {:error, :invalid_origin} = WireEnvelope.normalize_origin(["node@host"])

    assert {:error, :invalid_origin} =
             WireEnvelope.normalize_origin(9_223_372_036_854_775_808)
  end

  test "validates keys and encoded values at exact configured boundaries" do
    max_key = :binary.copy("k", WireEnvelope.max_key_bytes())
    max_value = :binary.copy(<<0>>, WireEnvelope.max_encoded_value_bytes())

    assert WireEnvelope.valid_key?("")
    assert WireEnvelope.valid_key?(max_key)
    refute WireEnvelope.valid_key?(max_key <> "k")
    refute WireEnvelope.valid_key?(:key)
    assert {:ok, byte_size(max_value)} == WireEnvelope.value_size(max_value)
    assert {:error, :value_too_large} = WireEnvelope.value_size(max_value <> <<0>>)
    assert {:error, :invalid_value} = WireEnvelope.value_size(:value)
  end

  test "entry and progress sizing measures canonical ETF terms" do
    entry = {"key", "value", @max_int64, "origin", @max_int64, @max_int64}
    replication_entry = {"key", "value", @max_int64, @max_int64, @max_int64, @max_int64}
    progress = %{"a" => 0, :bb => 1, 33 => 2}
    normalized_progress = %{"a" => 0, "bb" => 1, "33" => 2}

    assert {:ok, :erlang.external_size(entry)} ==
             WireEnvelope.entry_size("key", "value", "origin")

    assert {:ok, :erlang.external_size(replication_entry)} ==
             WireEnvelope.replication_entry_size("key", "value")

    assert {:ok, :erlang.external_size(normalized_progress)} ==
             WireEnvelope.progress_size(progress)
  end

  test "compressed entry sizing uses the wire limit before inflation" do
    wire_value =
      :binary.copy(<<0>>, WireEnvelope.max_encoded_value_bytes() + 1)

    assert {:error, :value_too_large} = WireEnvelope.entry_size("key", wire_value, "origin")
    assert {:ok, _bytes} = WireEnvelope.wire_entry_size("key", wire_value, "origin")

    assert {:error, :value_too_large} =
             WireEnvelope.replication_entry_size("key", wire_value)

    assert {:ok, _bytes} =
             WireEnvelope.wire_replication_entry_size("key", wire_value)
  end

  test "complete compressed v2 messages permit bounded compression overhead" do
    payload = :crypto.strong_rand_bytes(ValueCodec.max_encoded_bytes() - 64)
    value_binary = :erlang.term_to_binary(payload)
    from_node = :remote@host
    key = "large-value"
    origin = "remote-voter"
    timestamp = 1
    sequence = 1
    options = %{compress?: true, compression_threshold: 0}

    replication =
      {:ekv_replication_batch, from_node, 0, origin,
       [{key, value_binary, timestamp, sequence, nil, nil}]}

    assert {:ok,
            {:ekv, 2, :replication_batch,
             {^from_node, 0, ^origin,
              [{^key, {:ekv_wire_compressed, compressed}, ^timestamp, ^sequence, nil, nil}]}, %{}} =
              wire_replication} = WireProtocol.encode(replication, options)

    assert byte_size(value_binary) <= ValueCodec.max_encoded_bytes()
    assert byte_size(compressed) > ValueCodec.max_encoded_bytes()
    assert WireEnvelope.valid_message?(wire_replication)

    assert {:ok,
            {:replication_batch, ^from_node, 0, ^origin,
             [{^key, ^value_binary, ^timestamp, ^sequence, nil, nil}]}} =
             WireProtocol.decode(wire_replication)

    ref = make_ref()
    ballot = System.system_time(:nanosecond)
    entry = {key, value_binary, timestamp, origin, nil, nil}
    accept = {:ekv_accept, ref, self(), key, ballot, origin, entry, 0}

    assert {:ok, wire_accept} = WireProtocol.encode(accept, options)
    assert WireEnvelope.valid_message?(wire_accept)

    assert {:ok, {:accept, ^ref, _pid, ^key, ^ballot, ^origin, decoded_entry, 0}} =
             WireProtocol.decode(wire_accept)

    assert decoded_entry == entry
  end

  test "complete value-bearing messages have exact 8 MiB boundaries" do
    for {kind, builder} <- value_message_builders() do
      base_size = WireEnvelope.message_size(builder.(<<>>))
      boundary_value = :binary.copy(<<0>>, WireEnvelope.max_batch_bytes() - base_size)
      boundary_message = builder.(boundary_value)

      assert WireEnvelope.message_size(boundary_message) == WireEnvelope.max_batch_bytes(),
             "#{kind} did not land on the exact hard boundary"

      assert :ok = WireEnvelope.validate_message(boundary_message)

      assert {:error, :wire_message_too_large} =
               WireEnvelope.validate_message(builder.(boundary_value <> <<0>>))
    end
  end

  test "the configured value ceiling fits every complete value-bearing message" do
    max_value = :binary.copy(<<0>>, WireEnvelope.max_encoded_value_bytes())

    for {kind, builder} <- value_message_builders() do
      message = builder.(max_value)

      assert WireEnvelope.valid_message?(message),
             "#{kind} exceeded 8 MiB at the configured value ceiling: " <>
               "#{WireEnvelope.message_size(message)} bytes"
    end
  end

  test "progress and collection boundaries reject invalid metadata" do
    max_progress = Map.new(1..WireEnvelope.max_batch_entries(), &{&1, &1})
    assert {:ok, progress_bytes} = WireEnvelope.progress_size(max_progress)
    assert progress_bytes <= WireEnvelope.max_payload_bytes()

    oversized_progress = Map.put(max_progress, WireEnvelope.max_batch_entries() + 1, 0)
    assert {:error, :too_many_progress_entries} = WireEnvelope.progress_size(oversized_progress)
    assert {:error, :invalid_progress_sequence} = WireEnvelope.progress_size(%{"node" => -1})
    assert {:error, :invalid_progress} = WireEnvelope.progress_size([{"node", 1}])

    assert WireEnvelope.within_hard_batch?(
             WireEnvelope.max_batch_entries(),
             WireEnvelope.max_payload_bytes()
           )

    refute WireEnvelope.within_hard_batch?(WireEnvelope.max_batch_entries() + 1, 0)
    refute WireEnvelope.within_hard_batch?(0, WireEnvelope.max_payload_bytes() + 1)
  end

  test "bounds persisted integer metadata to SQLite int64" do
    max = 9_223_372_036_854_775_807
    min = -9_223_372_036_854_775_808

    assert WireEnvelope.valid_int64?(min)
    assert WireEnvelope.valid_int64?(max)
    refute WireEnvelope.valid_int64?(min - 1)
    refute WireEnvelope.valid_int64?(max + 1)
    assert WireEnvelope.valid_nonnegative_int64?(max)
    refute WireEnvelope.valid_nonnegative_int64?(-1)
    assert WireEnvelope.valid_entry_metadata?(min, 0, max, nil)
    refute WireEnvelope.valid_entry_metadata?(0, max + 1, nil, nil)
  end

  defp value_message_builders do
    key = :binary.copy("k", WireEnvelope.max_key_bytes())
    origin = :binary.copy("o", WireEnvelope.max_origin_bytes())
    request_id = make_ref()
    ref = make_ref()
    pid = self()

    entry = fn value -> {key, value, @max_int64 - 1, origin, nil, nil} end

    [
      replication_batch: fn value ->
        {:ekv, WireProtocol.version(), :replication_batch,
         {node(), 0, origin, [{key, value, @max_int64, @max_int64 - 1, nil, nil}]}, %{}}
      end,
      sync: fn value ->
        {:ekv, WireProtocol.version(), :sync,
         {node(), 0, request_id, :full,
          [{key, value, @max_int64, origin, @max_int64 - 1, nil, nil}], nil}, %{}}
      end,
      accept: fn value ->
        {:ekv, WireProtocol.version(), :accept,
         {ref, pid, key, @max_int64 - 1, origin, entry.(value), 0}, %{}}
      end,
      cas_committed: fn value ->
        {:ekv, WireProtocol.version(), :cas_committed,
         {pid, key, @max_int64 - 1, origin, entry.(value), 0, origin, @max_int64 - 1}, %{}}
      end,
      promise: fn value ->
        {:ekv, WireProtocol.version(), :promise,
         {ref, pid, origin, @max_int64 - 1, origin, [value, @max_int64 - 1, origin, nil, nil]},
         %{}}
      end
    ]
  end
end
