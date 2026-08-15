defmodule EKV.WireEnvelope do
  @moduledoc false

  @max_batch_entries 4096
  @max_batch_bytes 8 * 1024 * 1024
  @max_key_bytes 64 * 1024
  @max_origin_bytes 1024
  @message_overhead_reserve 256 * 1024
  @max_payload_bytes @max_batch_bytes - 64 * 1024
  @min_int64 -9_223_372_036_854_775_808
  @max_int64 9_223_372_036_854_775_807
  # CAS accept/commit carry the key both beside and inside the accepted entry.
  # This limit reserves both maximum keys, a maximum origin, and ample space for
  # the outer protocol tuple, refs, pids, ballots, collection framing, and meta.
  # Every final message is still measured exactly before send and after receive.
  @max_encoded_value_bytes @max_batch_bytes - 2 * @max_key_bytes - @max_origin_bytes -
                             @message_overhead_reserve

  @type error_reason ::
          :empty_origin
          | :invalid_key
          | :invalid_origin
          | :invalid_progress
          | :invalid_progress_sequence
          | :invalid_value
          | :key_too_large
          | :origin_too_large
          | :too_many_progress_entries
          | :value_too_large

  @spec max_batch_entries() :: pos_integer()
  def max_batch_entries, do: @max_batch_entries

  @spec max_batch_bytes() :: pos_integer()
  def max_batch_bytes, do: @max_batch_bytes

  @spec max_payload_bytes() :: pos_integer()
  def max_payload_bytes, do: @max_payload_bytes

  @spec max_key_bytes() :: pos_integer()
  def max_key_bytes, do: @max_key_bytes

  @spec max_origin_bytes() :: pos_integer()
  def max_origin_bytes, do: @max_origin_bytes

  @spec max_encoded_value_bytes() :: pos_integer()
  def max_encoded_value_bytes, do: @max_encoded_value_bytes

  @spec message_size(term()) :: non_neg_integer()
  def message_size(message), do: :erlang.external_size(message)

  @spec valid_message?(term()) :: boolean()
  def valid_message?(message), do: message_size(message) <= @max_batch_bytes

  @spec validate_message(term()) :: :ok | {:error, :wire_message_too_large}
  def validate_message(message) do
    if valid_message?(message), do: :ok, else: {:error, :wire_message_too_large}
  end

  @spec normalize_origin(term()) :: {:ok, binary()} | {:error, error_reason()}
  def normalize_origin(origin) when is_binary(origin), do: validate_origin(origin)

  def normalize_origin(origin) when is_atom(origin),
    do: origin |> Atom.to_string() |> validate_origin()

  def normalize_origin(origin)
      when is_integer(origin) and origin >= @min_int64 and origin <= @max_int64,
      do: origin |> Integer.to_string() |> validate_origin()

  def normalize_origin(_origin), do: {:error, :invalid_origin}

  @spec origin_size(term()) :: {:ok, pos_integer()} | {:error, error_reason()}
  def origin_size(origin) do
    with {:ok, normalized} <- normalize_origin(origin) do
      {:ok, byte_size(normalized)}
    end
  end

  @spec valid_key?(term()) :: boolean()
  def valid_key?(key), do: is_binary(key) and byte_size(key) <= @max_key_bytes

  @spec validate_key!(term()) :: binary()
  def validate_key!(key) do
    if valid_key?(key) do
      key
    else
      raise ArgumentError,
            "EKV key must be a binary no larger than #{@max_key_bytes} bytes"
    end
  end

  @spec value_size(term()) :: {:ok, non_neg_integer()} | {:error, error_reason()}
  def value_size(nil), do: {:ok, 0}

  def value_size(value) when is_binary(value) and byte_size(value) <= @max_encoded_value_bytes,
    do: {:ok, byte_size(value)}

  def value_size(value) when is_binary(value), do: {:error, :value_too_large}
  def value_size(_value), do: {:error, :invalid_value}

  @spec wire_value_size(term()) :: {:ok, non_neg_integer()} | {:error, error_reason()}
  def wire_value_size(nil), do: {:ok, 0}

  def wire_value_size(value) when is_binary(value) and byte_size(value) <= @max_batch_bytes,
    do: {:ok, byte_size(value)}

  def wire_value_size(value) when is_binary(value), do: {:error, :value_too_large}
  def wire_value_size(_value), do: {:error, :invalid_value}

  @spec entry_size(term(), term(), term()) ::
          {:ok, non_neg_integer()} | {:error, error_reason()}
  def entry_size(key, value, origin), do: entry_size(key, value, origin, &value_size/1)

  @spec wire_entry_size(term(), term(), term()) ::
          {:ok, non_neg_integer()} | {:error, error_reason()}
  def wire_entry_size(key, value, origin),
    do: entry_size(key, value, origin, &wire_value_size/1)

  defp entry_size(key, value, origin, value_size_fun) do
    with {:ok, _key_bytes} <- key_size(key),
         {:ok, _value_bytes} <- value_size_fun.(value),
         {:ok, origin} <- normalize_origin(origin) do
      {:ok, :erlang.external_size({key, value, @max_int64, origin, @max_int64, @max_int64})}
    end
  end

  @spec replication_entry_size(term(), term()) ::
          {:ok, non_neg_integer()} | {:error, error_reason()}
  def replication_entry_size(key, value),
    do: replication_entry_size(key, value, &value_size/1)

  @spec wire_replication_entry_size(term(), term()) ::
          {:ok, non_neg_integer()} | {:error, error_reason()}
  def wire_replication_entry_size(key, value),
    do: replication_entry_size(key, value, &wire_value_size/1)

  defp replication_entry_size(key, value, value_size_fun) do
    with {:ok, _key_bytes} <- key_size(key),
         {:ok, _value_bytes} <- value_size_fun.(value) do
      {:ok, :erlang.external_size({key, value, @max_int64, @max_int64, @max_int64, @max_int64})}
    end
  end

  @spec normalize_progress(term()) ::
          {:ok, map(), non_neg_integer()} | {:error, error_reason()}
  def normalize_progress(progress)
      when is_map(progress) and not is_struct(progress) and
             map_size(progress) <= @max_batch_entries do
    Enum.reduce_while(progress, {:ok, %{}}, fn {origin, sequence}, {:ok, normalized} ->
      with {:ok, origin} <- normalize_origin(origin),
           true <- valid_nonnegative_int64?(sequence) do
        {:cont, {:ok, Map.put(normalized, origin, sequence)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
        false -> {:halt, {:error, :invalid_progress_sequence}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, normalized, :erlang.external_size(normalized)}
      {:error, _reason} = error -> error
    end
  end

  def normalize_progress(progress) when is_map(progress) and not is_struct(progress),
    do: {:error, :too_many_progress_entries}

  def normalize_progress(_progress), do: {:error, :invalid_progress}

  @spec progress_size(term()) :: {:ok, non_neg_integer()} | {:error, error_reason()}
  def progress_size(progress) do
    with {:ok, _normalized, bytes} <- normalize_progress(progress), do: {:ok, bytes}
  end

  @spec within_hard_batch?(term(), term(), pos_integer()) :: boolean()
  def within_hard_batch?(count, bytes, byte_limit \\ @max_payload_bytes) do
    is_integer(count) and count >= 0 and count <= @max_batch_entries and
      is_integer(bytes) and bytes >= 0 and bytes <= byte_limit and byte_limit <= @max_batch_bytes
  end

  @spec valid_int64?(term()) :: boolean()
  def valid_int64?(value),
    do: is_integer(value) and value >= @min_int64 and value <= @max_int64

  @spec valid_nonnegative_int64?(term()) :: boolean()
  def valid_nonnegative_int64?(value),
    do: is_integer(value) and value >= 0 and value <= @max_int64

  @spec valid_entry_metadata?(term(), term(), term(), term()) :: boolean()
  def valid_entry_metadata?(timestamp, origin_seq, expires_at, deleted_at) do
    valid_int64?(timestamp) and valid_nonnegative_int64?(origin_seq) and
      optional_int64?(expires_at) and optional_int64?(deleted_at)
  end

  defp key_size(key) when is_binary(key) and byte_size(key) <= @max_key_bytes,
    do: {:ok, byte_size(key)}

  defp key_size(key) when is_binary(key), do: {:error, :key_too_large}
  defp key_size(_key), do: {:error, :invalid_key}

  defp validate_origin(""), do: {:error, :empty_origin}

  defp validate_origin(origin) when byte_size(origin) <= @max_origin_bytes,
    do: {:ok, origin}

  defp validate_origin(_origin), do: {:error, :origin_too_large}

  defp optional_int64?(nil), do: true
  defp optional_int64?(value), do: valid_int64?(value)
end
