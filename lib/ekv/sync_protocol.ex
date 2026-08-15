defmodule EKV.SyncProtocol do
  @moduledoc false

  alias EKV.{ValueCodec, WireEnvelope}

  def normalize_payload(
        from_node,
        request_id,
        entries,
        progress,
        mode,
        remote_shard,
        local_shard,
        num_shards,
        expected_request
      )
      when is_atom(from_node) and mode in [:delta, :full] and remote_shard == local_shard and
             is_list(entries) do
    with {:ok, delta_cursor} <- normalize_expected_request(mode, expected_request, request_id),
         {:ok, entries, terminal_cursor, entry_bytes} <-
           normalize_entries(entries, mode, local_shard, num_shards, delta_cursor),
         {:ok, progress, progress_bytes, terminal?} <- normalize_progress(progress),
         true <-
           WireEnvelope.within_hard_batch?(
             length(entries),
             entry_bytes + progress_bytes,
             WireEnvelope.max_payload_bytes()
           ),
         :ok <- validate_progress_contract(mode, terminal_cursor, progress, terminal?) do
      {:ok, entries, progress, terminal?}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_sync_envelope}
    end
  end

  def normalize_payload(
        _from_node,
        _request_id,
        _entries,
        _progress,
        _mode,
        _remote_shard,
        _local_shard,
        _num_shards,
        _expected_request
      ),
      do: {:error, :invalid_sync_envelope}

  def take_entries_by_limits(entries, max_entries, max_bytes, hard_max_bytes, byte_fun) do
    {selected_rev, selected_count, selected_bytes, stopped_early?, oversized?} =
      Enum.reduce_while(entries, {[], 0, 0, false, false}, fn
        entry, {acc, count, bytes, _stopped?, _oversized?} ->
          entry_bytes = byte_fun.(entry)

          cond do
            entry_bytes > hard_max_bytes ->
              {:halt, {acc, count, bytes, true, true}}

            count >= max_entries ->
              {:halt, {acc, count, bytes, true, false}}

            count > 0 and bytes + entry_bytes > max_bytes ->
              {:halt, {acc, count, bytes, true, false}}

            bytes + entry_bytes > hard_max_bytes ->
              {:halt, {acc, count, bytes, true, false}}

            true ->
              {:cont, {[entry | acc], count + 1, bytes + entry_bytes, false, false}}
          end
      end)

    selected = Enum.reverse(selected_rev)
    {selected, selected_bytes, stopped_early? or selected_count < length(entries), oversized?}
  end

  def entry_bytes({key, value, _timestamp, origin, _origin_seq, _expires_at, _deleted_at}) do
    {:ok, bytes} = WireEnvelope.entry_size(key, value, origin)
    bytes
  end

  def progress_bytes(progress) do
    {:ok, bytes} = WireEnvelope.progress_size(progress)
    bytes
  end

  defp normalize_expected_request(mode, %{request: request, id: id}, id),
    do: expected_cursor(mode, request)

  if Mix.env() == :test do
    # Raw test messages predate request references. Versioned member traffic
    # always carries an opaque request reference.
    defp normalize_expected_request(mode, request, :legacy), do: expected_cursor(mode, request)
  end

  defp normalize_expected_request(:full, _expected, _request_id),
    do: {:error, :unexpected_full_sync}

  defp normalize_expected_request(:delta, _expected, _request_id),
    do: {:error, :unexpected_delta_sync}

  defp expected_cursor(:full, request) do
    if full_request?(request) or delta_request?(request) do
      {:ok, nil}
    else
      {:error, :unexpected_full_sync}
    end
  end

  defp expected_cursor(:delta, {:delta, origin, from_seq}) do
    with {:ok, origin} <- WireEnvelope.normalize_origin(origin),
         true <- WireEnvelope.valid_nonnegative_int64?(from_seq) do
      {:ok, {origin, from_seq}}
    else
      _ -> {:error, :unexpected_delta_sync}
    end
  end

  defp expected_cursor(:delta, _request), do: {:error, :unexpected_delta_sync}

  defp full_request?(:full), do: true
  defp full_request?({:full, _reason}), do: true
  defp full_request?(_request), do: false

  defp delta_request?({:delta, _origin, _from_seq}), do: true
  defp delta_request?(_request), do: false

  defp normalize_entries(entries, mode, shard, num_shards, delta_cursor) do
    Enum.reduce_while(entries, {:ok, [], 0, 0, delta_cursor}, fn
      {key, value, timestamp, origin, origin_seq, expires_at, deleted_at},
      {:ok, acc, count, bytes, previous_delta}
      when is_binary(key) and is_integer(timestamp) and is_integer(origin_seq) and origin_seq >= 0 and
             (is_binary(origin) or is_atom(origin) or is_integer(origin)) and
             (is_nil(expires_at) or is_integer(expires_at)) and
             (is_nil(deleted_at) or is_integer(deleted_at)) ->
        with {:ok, origin} <- WireEnvelope.normalize_origin(origin),
             :ok <- validate_delta_sequence(mode, previous_delta, origin, origin_seq),
             true <-
               WireEnvelope.valid_entry_metadata?(timestamp, origin_seq, expires_at, deleted_at),
             true <- WireEnvelope.valid_key?(key),
             {:ok, entry_bytes} <- WireEnvelope.entry_size(key, value, origin),
             next_count = count + 1,
             next_bytes = bytes + entry_bytes,
             true <-
               WireEnvelope.within_hard_batch?(
                 next_count,
                 next_bytes,
                 WireEnvelope.max_payload_bytes()
               ),
             true <- :erlang.phash2(key, num_shards) == shard,
             true <- ValueCodec.valid_persisted_value?(value, deleted_at) do
          entry = {key, value, timestamp, origin, origin_seq, expires_at, deleted_at}
          next_delta = if mode == :delta, do: {origin, origin_seq}, else: nil
          {:cont, {:ok, [entry | acc], next_count, next_bytes, next_delta}}
        else
          _invalid -> {:halt, {:error, :invalid_sync_entry}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_sync_entry}}
    end)
    |> case do
      {:ok, normalized, _count, bytes, terminal_cursor} ->
        {:ok, Enum.reverse(normalized), terminal_cursor, bytes}

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_delta_sequence(:full, _previous, _origin, _origin_seq), do: :ok
  defp validate_delta_sequence(:delta, nil, _origin, _origin_seq), do: :ok

  defp validate_delta_sequence(:delta, {origin, previous_seq}, origin, origin_seq)
       when origin_seq == previous_seq + 1,
       do: :ok

  defp validate_delta_sequence(:delta, _previous, _origin, _origin_seq),
    do: {:error, :invalid_delta_sequence}

  defp normalize_progress(nil), do: {:ok, %{}, 0, false}

  defp normalize_progress(progress) when is_map(progress) do
    with {:ok, normalized, progress_bytes} <- WireEnvelope.normalize_progress(progress) do
      {:ok, normalized, progress_bytes, true}
    end
  end

  defp normalize_progress(_progress), do: {:error, :invalid_progress_summary}

  defp validate_progress_contract(:full, _terminal_cursor, _progress, _terminal?), do: :ok
  defp validate_progress_contract(:delta, _terminal_cursor, _progress, false), do: :ok

  defp validate_progress_contract(:delta, {origin, terminal_seq}, progress, true)
       when map_size(progress) == 1 do
    case progress do
      %{^origin => ^terminal_seq} -> :ok
      _progress -> {:error, :invalid_delta_progress}
    end
  end

  defp validate_progress_contract(:delta, _terminal_cursor, _progress, true),
    do: {:error, :invalid_delta_progress}
end
