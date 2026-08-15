defmodule EKV.CAS.Protocol do
  @moduledoc false

  alias EKV.{ValueCodec, WireEnvelope}
  alias EKV.CAS.Ballot

  @min_signed_64 -9_223_372_036_854_775_808

  def validate_request(context, ref, proposer_pid, key, ballot_c, ballot_n, shard) do
    with true <- is_reference(ref),
         true <- shard == context.shard_index,
         true <- WireEnvelope.valid_key?(key),
         true <- Ballot.valid_node_id?(context.local_node_id),
         true <- Ballot.valid_incoming?(ballot_c, ballot_n),
         {:ok, ^ballot_n} <- context.known_voter_identity.(proposer_pid) do
      :ok
    else
      _invalid -> {:error, :invalid_cas_request}
    end
  end

  def validate_commit_sender(context, proposer_pid, key, ballot_c, ballot_n, shard) do
    with true <- shard == context.shard_index,
         true <- WireEnvelope.valid_key?(key),
         true <- Ballot.valid_incoming?(ballot_c, ballot_n),
         {:ok, ^ballot_n} <- context.known_voter_identity.(proposer_pid) do
      :ok
    else
      _invalid -> {:error, :invalid_cas_commit_sender}
    end
  end

  def validate_response_voter(known_voter_identity, pid, remote_node_id) do
    with true <- Ballot.valid_node_id?(remote_node_id),
         {:ok, ^remote_node_id} <- known_voter_identity.(pid) do
      :ok
    else
      _invalid -> {:error, :invalid_cas_response_voter}
    end
  end

  def validate_promise_payload(op, acc_c, acc_n, kv_row) do
    with true <- Ballot.valid_accepted?(acc_c, acc_n, op.ballot),
         {:ok, kv_row} <- validate_promise_row(kv_row, op.key) do
      {:ok, kv_row}
    else
      false -> {:error, :invalid_accepted_ballot}
      {:error, _reason} = error -> error
    end
  end

  def validate_promise_row(nil, _key), do: {:ok, nil}

  def validate_promise_row([value_binary, timestamp, origin_node, expires_at, deleted_at], key) do
    validate_entry_tuple(
      {key, value_binary, timestamp, origin_node, expires_at, deleted_at},
      key
    )
  end

  def validate_promise_row(_kv_row, _key), do: {:error, :invalid_promise_row}

  def validate_entry_tuple(
        {entry_key, value_binary, timestamp, origin_node, expires_at, deleted_at},
        expected_key
      )
      when entry_key == expected_key and is_integer(timestamp) and
             (is_nil(expires_at) or is_integer(expires_at)) and
             (is_nil(deleted_at) or is_integer(deleted_at)) do
    with true <- WireEnvelope.valid_key?(entry_key),
         {:ok, origin_node} <- WireEnvelope.normalize_origin(origin_node),
         true <- valid_timestamp?(timestamp),
         true <- WireEnvelope.valid_entry_metadata?(timestamp, 0, expires_at, deleted_at),
         {:ok, _entry_bytes} <- WireEnvelope.entry_size(entry_key, value_binary, origin_node),
         true <- ValueCodec.valid_persisted_value?(value_binary, deleted_at) do
      {:ok, [value_binary, timestamp, origin_node, expires_at, deleted_at]}
    else
      false -> {:error, :invalid_entry_envelope}
      {:error, _reason} = error -> error
    end
  end

  def validate_entry_tuple(_entry_tuple, _expected_key), do: {:error, :invalid_entry_shape}

  def valid_timestamp?(timestamp),
    do:
      is_integer(timestamp) and timestamp >= @min_signed_64 and timestamp <= Ballot.max_counter()

  def valid_sequence?(sequence),
    do: is_integer(sequence) and sequence >= 0 and sequence <= Ballot.max_counter()
end
