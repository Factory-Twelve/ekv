defmodule EKV.WireProtocol do
  @moduledoc false

  alias EKV.{ValueCodec, WireEnvelope}
  alias EKV.CAS.Ballot

  @version 2
  @compressed_tag :ekv_wire_compressed
  @features MapSet.new([:live_progress, :wire_compression, :observer])
  @best_effort_kinds [
    :replication_batch,
    :member_connect,
    :member_connect_ack,
    :summary_probe,
    :summary_reply,
    :sync_request,
    :progress_ack
  ]

  def version, do: @version

  def local_features(cas_voter?) when is_boolean(cas_voter?) do
    %{
      live_progress: true,
      wire_compression: true,
      observer: not cas_voter?
    }
  end

  def best_effort?({:ekv, @version, kind, _payload, _meta}),
    do: kind in @best_effort_kinds

  def best_effort?(_message), do: false

  def encode(message, options) when is_map(options) do
    with {:ok, kind, payload, meta} <- encode_payload(message, options),
         encoded = {:ekv, @version, kind, payload, meta},
         :ok <- WireEnvelope.validate_message(encoded) do
      {:ok, encoded}
    end
  end

  def encode(_message, _options), do: {:error, :invalid_wire_options}

  def decode({:ekv, @version, kind, payload, meta} = message) do
    with :ok <- WireEnvelope.validate_message(message),
         true <- is_map(meta),
         {:ok, decoded} <- decode_payload(kind, payload, meta) do
      {:ok, decoded}
    else
      false -> {:error, :invalid_wire_meta}
      :ignore -> :ignore
      {:error, _reason} = error -> error
    end
  end

  def decode({:ekv, version, kind, _payload, _meta}) when is_integer(version),
    do: {:unsupported_version, version, kind}

  def decode(_message), do: {:error, :invalid_wire_envelope}

  # Raw tuples are compiled only into the test build for focused state-machine
  # injection. Production member traffic must use decode/1 and protocol v2.
  if Mix.env() == :test do
    def decode_local_compat({:ekv_replication_batch, from_node, shard, origin, entries}),
      do: decode_payload(:replication_batch, {from_node, shard, origin, entries}, %{})

    def decode_local_compat({:ekv_member_connect, pid, shard, num_shards, progress, node_id}),
      do: decode_payload(:member_connect, {pid, shard, num_shards, progress, node_id}, %{})

    def decode_local_compat(
          {:ekv_member_connect, pid, shard, num_shards, progress, node_id, features}
        ),
        do:
          decode_payload(
            :member_connect,
            {pid, shard, num_shards, progress, node_id},
            %{features: features}
          )

    def decode_local_compat({:ekv_member_connect_ack, pid, shard, num_shards, progress, node_id}),
      do: decode_payload(:member_connect_ack, {pid, shard, num_shards, progress, node_id}, %{})

    def decode_local_compat(
          {:ekv_member_connect_ack, pid, shard, num_shards, progress, node_id, features}
        ),
        do:
          decode_payload(
            :member_connect_ack,
            {pid, shard, num_shards, progress, node_id},
            %{features: features}
          )

    def decode_local_compat({:ekv_summary_probe, pid, shard, progress}),
      do: decode_payload(:summary_probe, {pid, shard, progress}, %{})

    def decode_local_compat({:ekv_summary_probe, pid, shard, progress, node_id}),
      do: decode_payload(:summary_probe, {pid, shard, progress}, %{node_id: node_id})

    def decode_local_compat({:ekv_summary_reply, pid, shard, progress}),
      do: decode_payload(:summary_reply, {pid, shard, progress}, %{})

    def decode_local_compat({:ekv_summary_reply, pid, shard, progress, node_id}),
      do: decode_payload(:summary_reply, {pid, shard, progress}, %{node_id: node_id})

    def decode_local_compat({:ekv_sync_request, pid, shard, request}),
      do: decode_local_sync_request(pid, shard, request, :legacy)

    def decode_local_compat({:ekv_sync_request, pid, shard, request, request_id}),
      do: decode_local_sync_request(pid, shard, request, request_id)

    def decode_local_compat({:ekv_sync, from_node, shard, mode, entries, progress}),
      do: decode_local_sync(from_node, shard, :legacy, mode, entries, progress)

    def decode_local_compat({:ekv_sync, from_node, shard, request_id, mode, entries, progress}),
      do: decode_local_sync(from_node, shard, request_id, mode, entries, progress)

    def decode_local_compat({:ekv_progress_ack, pid, shard, mode, progress}),
      do: decode_payload(:progress_ack, {pid, shard, mode, progress}, %{})

    def decode_local_compat({:ekv_prepare, ref, pid, key, counter, node_id, shard}),
      do: decode_payload(:prepare, {ref, pid, key, counter, node_id, shard}, %{})

    def decode_local_compat({:ekv_accept, ref, pid, key, counter, node_id, entry, shard}),
      do: decode_payload(:accept, {ref, pid, key, counter, node_id, entry, shard}, %{})

    def decode_local_compat(
          {:ekv_cas_committed, pid, key, counter, node_id, entry, shard, origin, sequence}
        ),
        do:
          decode_payload(
            :cas_committed,
            {pid, key, counter, node_id, entry, shard, origin, sequence},
            %{}
          )

    def decode_local_compat({:ekv_promise, ref, pid, node_id, counter, accepted_by, row}),
      do: decode_payload(:promise, {ref, pid, node_id, counter, accepted_by, row}, %{})

    def decode_local_compat({:ekv_nack, ref, pid, node_id, counter, promised_by}),
      do: decode_payload(:nack, {ref, pid, node_id, counter, promised_by}, %{})

    def decode_local_compat({:ekv_accepted, ref, pid, node_id}),
      do: decode_payload(:accepted, {ref, pid, node_id}, %{})

    def decode_local_compat({:ekv_accept_nack, ref, pid, node_id}),
      do: decode_payload(:accept_nack, {ref, pid, node_id}, %{})

    def decode_local_compat(_message), do: :ignore
  end

  def normalize_progress(progress) do
    with {:ok, normalized, _bytes} <- WireEnvelope.normalize_progress(progress) do
      {:ok, normalized}
    end
  end

  def normalize_features(%MapSet{} = features) do
    with {:ok, feature_list} <- safe_map_set_to_list(features),
         true <- Enum.all?(feature_list, &MapSet.member?(@features, &1)) do
      {:ok, MapSet.new(feature_list)}
    else
      _invalid -> {:error, :invalid_features}
    end
  end

  def normalize_features(features) when is_map(features) and not is_struct(features) do
    if Enum.all?(features, fn {feature, enabled?} ->
         MapSet.member?(@features, feature) and is_boolean(enabled?)
       end) do
      normalized =
        features
        |> Enum.filter(fn {_feature, enabled?} -> enabled? end)
        |> Enum.map(&elem(&1, 0))
        |> MapSet.new()

      {:ok, normalized}
    else
      {:error, :invalid_features}
    end
  end

  def normalize_features(_features), do: {:error, :invalid_features}

  defp encode_payload(
         {:ekv_replication_batch, from_node, shard, origin, entries},
         options
       ) do
    with {:ok, entries} <- compress_replication_entries(entries, options) do
      {:ok, :replication_batch, {from_node, shard, origin, entries}, %{}}
    end
  end

  defp encode_payload(
         {:ekv_accept, ref, proposer_pid, key, counter, node_id, entry, shard},
         options
       ) do
    with {:ok, entry} <- compress_entry(entry, options) do
      {:ok, :accept, {ref, proposer_pid, key, counter, node_id, entry, shard}, %{}}
    end
  end

  defp encode_payload(
         {:ekv_cas_committed, proposer_pid, key, counter, node_id, entry, shard, origin,
          sequence},
         options
       ) do
    with {:ok, entry} <- compress_entry(entry, options) do
      {:ok, :cas_committed, {proposer_pid, key, counter, node_id, entry, shard, origin, sequence},
       %{}}
    end
  end

  defp encode_payload(
         {:ekv_member_connect, pid, shard, num_shards, progress, node_id},
         options
       ) do
    {:ok, :member_connect, {pid, shard, num_shards, progress, node_id}, feature_meta(options)}
  end

  defp encode_payload(
         {:ekv_member_connect_ack, pid, shard, num_shards, progress, node_id},
         options
       ) do
    {:ok, :member_connect_ack, {pid, shard, num_shards, progress, node_id}, feature_meta(options)}
  end

  defp encode_payload(
         {:ekv_sync, from_node, shard, request_id, mode, entries, progress},
         _options
       ),
       do: {:ok, :sync, {from_node, shard, request_id, mode, entries, progress}, %{}}

  defp encode_payload({:ekv_summary_probe, pid, shard, progress, node_id}, _options),
    do: {:ok, :summary_probe, {pid, shard, progress}, optional_node_meta(node_id)}

  defp encode_payload({:ekv_summary_reply, pid, shard, progress, node_id}, _options),
    do: {:ok, :summary_reply, {pid, shard, progress}, optional_node_meta(node_id)}

  defp encode_payload({:ekv_summary_probe, pid, shard, progress}, _options),
    do: {:ok, :summary_probe, {pid, shard, progress}, %{}}

  defp encode_payload({:ekv_summary_reply, pid, shard, progress}, _options),
    do: {:ok, :summary_reply, {pid, shard, progress}, %{}}

  defp encode_payload(
         {:ekv_sync_request, pid, shard, {:full, reason}, request_id},
         _options
       ),
       do: {:ok, :sync_request, {pid, shard, :full, request_id}, %{explicit_full_reason: reason}}

  defp encode_payload({:ekv_sync_request, pid, shard, request, request_id}, _options),
    do: {:ok, :sync_request, {pid, shard, request, request_id}, %{}}

  defp encode_payload({:ekv_progress_ack, pid, shard, mode, progress}, _options),
    do: {:ok, :progress_ack, {pid, shard, mode, progress}, %{}}

  defp encode_payload({:ekv_prepare, ref, pid, key, counter, node_id, shard}, _options),
    do: {:ok, :prepare, {ref, pid, key, counter, node_id, shard}, %{}}

  defp encode_payload({:ekv_promise, ref, pid, node_id, counter, accepted_by, row}, _options),
    do: {:ok, :promise, {ref, pid, node_id, counter, accepted_by, row}, %{}}

  defp encode_payload({:ekv_nack, ref, pid, node_id, counter, promised_by}, _options),
    do: {:ok, :nack, {ref, pid, node_id, counter, promised_by}, %{}}

  defp encode_payload({:ekv_accepted, ref, pid, node_id}, _options),
    do: {:ok, :accepted, {ref, pid, node_id}, %{}}

  defp encode_payload({:ekv_accept_nack, ref, pid, node_id}, _options),
    do: {:ok, :accept_nack, {ref, pid, node_id}, %{}}

  defp encode_payload(_message, _options), do: {:error, :unsupported_wire_message}

  defp decode_payload(:replication_batch, {from_node, shard, origin, entries}, _meta) do
    with true <- is_atom(from_node),
         true <- valid_shard?(shard),
         {:ok, origin} <- WireEnvelope.normalize_origin(origin),
         {:ok, entries} <- normalize_replication_entries(entries, origin) do
      {:ok, {:replication_batch, from_node, shard, origin, entries}}
    else
      false -> {:error, :invalid_replication_header}
      {:error, _reason} = error -> error
    end
  end

  defp decode_payload(:member_connect, payload, meta),
    do: decode_handshake(:member_connect, payload, meta)

  defp decode_payload(:member_connect_ack, payload, meta),
    do: decode_handshake(:member_connect_ack, payload, meta)

  defp decode_payload(:sync, {from_node, shard, request_id, mode, entries, progress}, _meta) do
    with true <- is_atom(from_node),
         true <- valid_shard?(shard),
         true <- is_reference(request_id),
         true <- mode in [:full, :delta],
         true <- is_list(entries) do
      {:ok, {:sync, from_node, shard, request_id, mode, entries, progress}}
    else
      false -> {:error, :invalid_sync_header}
    end
  end

  defp decode_payload(:summary_probe, {pid, shard, progress}, meta),
    do: normalize_summary(:summary_probe, pid, shard, progress, meta)

  defp decode_payload(:summary_reply, {pid, shard, progress}, meta),
    do: normalize_summary(:summary_reply, pid, shard, progress, meta)

  defp decode_payload(:sync_request, {pid, shard, request, request_id}, meta) do
    with {:ok, pid, shard, request, request_id} <-
           normalize_sync_request(pid, shard, request, request_id),
         {:ok, request} <- attach_full_reason(request, meta) do
      {:ok, {:sync_request, pid, shard, request, request_id}}
    end
  end

  defp decode_payload(:progress_ack, {pid, shard, mode, progress}, _meta) do
    with {:ok, progress} <- normalize_progress_ack(pid, shard, mode, progress) do
      {:ok, {:progress_ack, pid, shard, mode, progress}}
    end
  end

  defp decode_payload(:prepare, {ref, pid, key, counter, node_id, shard}, _meta),
    do: {:ok, {:prepare, ref, pid, key, counter, node_id, shard}}

  defp decode_payload(:accept, {ref, pid, key, counter, node_id, entry, shard}, _meta) do
    with {:ok, entry} <- decompress_entry(entry) do
      {:ok, {:accept, ref, pid, key, counter, node_id, entry, shard}}
    end
  end

  defp decode_payload(
         :cas_committed,
         {pid, key, counter, node_id, entry, shard, origin, sequence},
         _meta
       ) do
    with {:ok, origin} <- WireEnvelope.normalize_origin(origin),
         {:ok, entry} <- decompress_entry(entry) do
      {:ok, {:cas_committed, pid, key, counter, node_id, entry, shard, origin, sequence}}
    end
  end

  defp decode_payload(:promise, {ref, pid, node_id, counter, accepted_by, row}, _meta),
    do: {:ok, {:promise, ref, pid, node_id, counter, accepted_by, row}}

  defp decode_payload(:nack, {ref, pid, node_id, counter, promised_by}, _meta),
    do: {:ok, {:nack, ref, pid, node_id, counter, promised_by}}

  defp decode_payload(:accepted, {ref, pid, node_id}, _meta),
    do: {:ok, {:accepted, ref, pid, node_id}}

  defp decode_payload(:accept_nack, {ref, pid, node_id}, _meta),
    do: {:ok, {:accept_nack, ref, pid, node_id}}

  defp decode_payload(_kind, _payload, _meta), do: :ignore

  defp decode_handshake(kind, {pid, shard, num_shards, progress, node_id}, meta) do
    with {:ok, features} <- normalize_feature_meta(meta),
         true <- is_pid(pid),
         true <- valid_shard?(shard),
         true <- is_integer(num_shards) and num_shards > 0,
         true <- Ballot.valid_node_id?(node_id),
         {:ok, progress} <- normalize_progress(progress) do
      {:ok, {kind, pid, shard, num_shards, progress, node_id, features}}
    else
      _invalid -> {:error, :invalid_member_handshake}
    end
  end

  defp decode_handshake(_kind, _payload, _meta), do: {:error, :invalid_member_handshake}

  defp normalize_progress_ack(pid, shard, mode, progress) do
    with true <- is_pid(pid),
         true <- valid_shard?(shard),
         true <- mode in [:full, :delta],
         {:ok, progress} <- normalize_progress(progress) do
      {:ok, progress}
    else
      _invalid -> {:error, :invalid_progress_ack}
    end
  end

  defp normalize_feature_meta(%{features: features} = meta) when map_size(meta) == 1,
    do: normalize_features(features)

  defp normalize_feature_meta(meta) when map_size(meta) == 0, do: {:ok, MapSet.new()}
  defp normalize_feature_meta(_meta), do: {:error, :invalid_feature_meta}

  defp normalize_summary(kind, pid, shard, progress, meta) do
    with true <- is_pid(pid),
         true <- valid_shard?(shard),
         {:ok, progress} <- normalize_progress(progress),
         {:ok, node_id} <- normalize_optional_node_id(meta) do
      {:ok, {kind, pid, shard, progress, node_id}}
    else
      _invalid -> {:error, :invalid_summary_message}
    end
  end

  defp normalize_optional_node_id(%{node_id: node_id} = meta) when map_size(meta) == 1 do
    if Ballot.valid_node_id?(node_id), do: {:ok, node_id}, else: {:error, :invalid_node_id}
  end

  defp normalize_optional_node_id(meta) when map_size(meta) == 0, do: {:ok, nil}
  defp normalize_optional_node_id(_meta), do: {:error, :invalid_summary_meta}

  defp normalize_sync_request(pid, shard, request, request_id) do
    with true <- is_pid(pid),
         true <- valid_shard?(shard),
         true <- is_reference(request_id),
         true <- valid_sync_request?(request) do
      {:ok, pid, shard, request, request_id}
    else
      _invalid -> {:error, :invalid_sync_request}
    end
  end

  if Mix.env() == :test do
    defp decode_local_sync_request(pid, shard, request, :legacy) do
      with true <- is_pid(pid),
           true <- valid_shard?(shard),
           true <- valid_sync_request?(request) do
        {:ok, {:sync_request, pid, shard, request, :legacy}}
      else
        _invalid -> {:error, :invalid_sync_request}
      end
    end

    defp decode_local_sync_request(pid, shard, request, request_id),
      do: decode_payload(:sync_request, {pid, shard, request, request_id}, %{})

    defp decode_local_sync(from_node, shard, :legacy, mode, entries, progress) do
      with true <- is_atom(from_node),
           true <- valid_shard?(shard),
           true <- mode in [:full, :delta],
           true <- is_list(entries) do
        {:ok, {:sync, from_node, shard, :legacy, mode, entries, progress}}
      else
        _invalid -> {:error, :invalid_sync_header}
      end
    end

    defp decode_local_sync(from_node, shard, request_id, mode, entries, progress),
      do: decode_payload(:sync, {from_node, shard, request_id, mode, entries, progress}, %{})
  end

  defp valid_sync_request?(:full), do: true

  defp valid_sync_request?({:delta, origin, from_seq}) do
    match?({:ok, _origin}, WireEnvelope.normalize_origin(origin)) and
      WireEnvelope.valid_nonnegative_int64?(from_seq)
  end

  defp valid_sync_request?(_request), do: false

  defp attach_full_reason(:full, %{explicit_full_reason: reason} = meta)
       when map_size(meta) == 1,
       do: {:ok, {:full, reason}}

  defp attach_full_reason(request, meta) when map_size(meta) == 0, do: {:ok, request}
  defp attach_full_reason(_request, _meta), do: {:error, :invalid_sync_request_meta}

  defp normalize_replication_entries(entries, origin) when is_list(entries) do
    if length(entries) <= WireEnvelope.max_batch_entries() do
      max_bytes = WireEnvelope.max_payload_bytes() - byte_size(origin)

      Enum.reduce_while(entries, {:ok, [], 0}, fn
        {key, wire_value, timestamp, sequence, expires_at, deleted_at}, {:ok, acc, bytes} ->
          with true <-
                 WireEnvelope.valid_entry_metadata?(
                   timestamp,
                   sequence,
                   expires_at,
                   deleted_at
                 ),
               {:ok, sized_value} <- value_for_size(wire_value),
               {:ok, wire_bytes} <-
                 WireEnvelope.wire_replication_entry_size(key, sized_value),
               true <- bytes + wire_bytes <= max_bytes,
               {:ok, value} <- decompress_value(wire_value),
               {:ok, expanded_bytes} <- WireEnvelope.replication_entry_size(key, value),
               true <- bytes + expanded_bytes <= max_bytes,
               true <- ValueCodec.valid_persisted_value?(value, deleted_at) do
            entry = {key, value, timestamp, sequence, expires_at, deleted_at}
            {:cont, {:ok, [entry | acc], bytes + expanded_bytes}}
          else
            {:error, _reason} = error -> {:halt, error}
            false -> {:halt, {:error, :invalid_replication_entry}}
          end

        _entry, _acc ->
          {:halt, {:error, :invalid_replication_entry}}
      end)
      |> case do
        {:ok, entries, _bytes} -> {:ok, Enum.reverse(entries)}
        {:error, _reason} = error -> error
      end
    else
      {:error, :invalid_entry_collection}
    end
  end

  defp normalize_replication_entries(_entries, _origin),
    do: {:error, :invalid_entry_collection}

  defp compress_replication_entries(entries, options) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {key, value, timestamp, sequence, expires_at, deleted_at}, {:ok, acc} ->
        with {:ok, value} <- compress_value(value, options) do
          entry = {key, value, timestamp, sequence, expires_at, deleted_at}
          {:cont, {:ok, [entry | acc]}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_entry_shape}}
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp compress_replication_entries(_entries, _options), do: {:error, :invalid_entry_collection}

  defp compress_entry(nil, _options), do: {:ok, nil}

  defp compress_entry({key, value, timestamp, origin, expires_at, deleted_at}, options) do
    with {:ok, value} <- compress_value(value, options) do
      {:ok, {key, value, timestamp, origin, expires_at, deleted_at}}
    end
  end

  defp compress_entry(_entry, _options), do: {:error, :invalid_entry_shape}

  defp decompress_entry(nil), do: {:ok, nil}

  defp decompress_entry({key, wire_value, timestamp, origin, expires_at, deleted_at}) do
    with {:ok, origin} <- WireEnvelope.normalize_origin(origin),
         {:ok, sized_value} <- value_for_size(wire_value),
         {:ok, wire_bytes} <- WireEnvelope.wire_entry_size(key, sized_value, origin),
         true <- wire_bytes <= WireEnvelope.max_batch_bytes(),
         {:ok, value} <- decompress_value(wire_value),
         {:ok, expanded_bytes} <- WireEnvelope.entry_size(key, value, origin),
         true <- expanded_bytes <= WireEnvelope.max_batch_bytes() do
      {:ok, {key, value, timestamp, origin, expires_at, deleted_at}}
    else
      false -> {:error, :wire_entry_too_large}
      {:error, _reason} = error -> error
    end
  end

  defp decompress_entry(_entry), do: {:error, :invalid_entry_shape}

  defp compress_value(nil, _options), do: {:ok, nil}

  defp compress_value(value, options) when is_binary(value) do
    if compression_enabled?(options) and compression_due?(value, options) do
      {:ok, {@compressed_tag, compress_binary(value)}}
    else
      {:ok, value}
    end
  end

  defp compress_value(_value, _options), do: {:error, :invalid_value_binary}

  defp decompress_value({@compressed_tag, compressed}) when is_binary(compressed),
    do: ValueCodec.decompress_wire(compressed)

  defp decompress_value(nil), do: {:ok, nil}
  defp decompress_value(value) when is_binary(value), do: {:ok, value}
  defp decompress_value(_value), do: {:error, :invalid_value_binary}

  defp value_for_size({@compressed_tag, compressed}) when is_binary(compressed),
    do: {:ok, compressed}

  defp value_for_size(value) when is_binary(value) or is_nil(value), do: {:ok, value}
  defp value_for_size(_value), do: {:error, :invalid_value_binary}

  defp compression_enabled?(options), do: Map.get(options, :compress?, false) == true

  defp compression_due?(value, options) do
    case Map.get(options, :compression_threshold) do
      threshold when is_integer(threshold) and threshold >= 0 -> byte_size(value) >= threshold
      _disabled -> false
    end
  end

  defp compress_binary(binary) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, 1)
      z |> :zlib.deflate(binary, :finish) |> IO.iodata_to_binary()
    after
      :zlib.close(z)
    end
  end

  defp feature_meta(options), do: %{features: Map.get(options, :features, %{})}

  defp optional_node_meta(node_id) do
    if Ballot.valid_node_id?(node_id), do: %{node_id: node_id}, else: %{}
  end

  defp safe_map_set_to_list(features) do
    try do
      {:ok, MapSet.to_list(features)}
    rescue
      _invalid_struct -> {:error, :invalid_features}
    catch
      _kind, _reason -> {:error, :invalid_features}
    end
  end

  defp valid_shard?(shard), do: is_integer(shard) and shard >= 0
end
