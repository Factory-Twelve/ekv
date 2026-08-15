defmodule EKV.ValueCodec do
  @moduledoc false

  alias EKV.WireEnvelope

  # Keep the encoded and wire-decompressed value ceiling aligned with the hard
  # per-message batch ceiling enforced by replication and sync receivers.
  @max_encoded_bytes WireEnvelope.max_encoded_value_bytes()
  @max_wire_compressed_bytes WireEnvelope.max_batch_bytes()
  @max_decoded_heap_words 1024 * 1024
  @external_term_version 131
  @compressed_term_tag 80

  @type decode_error ::
          :not_a_binary
          | :encoded_value_too_large
          | :compressed_external_term
          | :trailing_bytes
          | :decoded_value_too_large
          | :invalid_or_unsafe_external_term

  @spec max_encoded_bytes() :: pos_integer()
  def max_encoded_bytes, do: @max_encoded_bytes

  @spec max_decoded_heap_words() :: pos_integer()
  def max_decoded_heap_words, do: @max_decoded_heap_words

  @spec encode!(term(), term()) :: binary()
  def encode!(value, context \\ :encode) do
    value_binary = :erlang.term_to_binary(value)

    case decode(value_binary) do
      {:ok, ^value} ->
        value_binary

      {:ok, _other} ->
        raise ArgumentError, "EKV could not safely encode value for #{inspect(context)}"

      {:error, reason} ->
        raise ArgumentError,
              "EKV could not safely encode value for #{inspect(context)}: #{reason}"
    end
  end

  @spec decode(binary()) :: {:ok, term()} | {:error, decode_error()}
  def decode(value_binary) when not is_binary(value_binary), do: {:error, :not_a_binary}

  def decode(value_binary) when byte_size(value_binary) > @max_encoded_bytes,
    do: {:error, :encoded_value_too_large}

  def decode(<<@external_term_version, @compressed_term_tag, _rest::binary>>),
    do: {:error, :compressed_external_term}

  def decode(value_binary) do
    decode_isolated(value_binary)
  end

  defp decode_isolated(value_binary) do
    parent = self()
    reply_ref = make_ref()

    {_pid, monitor_ref} =
      :erlang.spawn_opt(
        fn -> send(parent, {reply_ref, decode_term(value_binary)}) end,
        [
          :monitor,
          {:message_queue_data, :off_heap},
          {:max_heap_size, %{size: @max_decoded_heap_words, kill: true, error_logger: false}}
        ]
      )

    receive do
      {^reply_ref, result} ->
        Process.demonitor(monitor_ref, [:flush])
        result

      {:DOWN, ^monitor_ref, :process, _pid, _reason} ->
        {:error, :decoded_value_too_large}
    end
  end

  defp decode_term(value_binary) do
    try do
      case :erlang.binary_to_term(value_binary, [:safe, :used]) do
        {value, used} when used == byte_size(value_binary) -> {:ok, value}
        {_value, _used} -> {:error, :trailing_bytes}
      end
    rescue
      ArgumentError -> {:error, :invalid_or_unsafe_external_term}
    end
  end

  @spec decode!(binary(), term()) :: term()
  def decode!(value_binary, context) do
    case decode(value_binary) do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise EKV.DecodeError,
          reason: reason,
          context: context,
          encoded_size: encoded_size(value_binary)
    end
  end

  @spec validate(binary()) :: :ok | {:error, decode_error()}
  def validate(value_binary) do
    case decode(value_binary) do
      {:ok, _value} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec valid_persisted_value?(binary() | nil, integer() | nil) :: boolean()
  def valid_persisted_value?(nil, deleted_at) when is_integer(deleted_at), do: true

  def valid_persisted_value?(value_binary, nil) when is_binary(value_binary),
    do: validate(value_binary) == :ok

  def valid_persisted_value?(_value_binary, _deleted_at), do: false

  @spec decompress_wire(binary()) :: {:ok, binary()} | {:error, atom()}
  def decompress_wire(compressed_binary) when not is_binary(compressed_binary),
    do: {:error, :invalid_wire_compression}

  def decompress_wire(compressed_binary)
      when byte_size(compressed_binary) > @max_wire_compressed_bytes,
      do: {:error, :compressed_value_too_large}

  def decompress_wire(compressed_binary) do
    z = :zlib.open()

    try do
      :ok = :zlib.inflateInit(z)
      inflate_bounded(z, compressed_binary, [], 0)
    catch
      :error, _reason -> {:error, :invalid_wire_compression}
      :exit, _reason -> {:error, :invalid_wire_compression}
    after
      :zlib.close(z)
    end
  end

  defp inflate_bounded(z, input, chunks, size) do
    case :zlib.safeInflate(z, input) do
      {status, output} when status in [:continue, :finished] ->
        output_size = IO.iodata_length(output)
        next_size = size + output_size

        cond do
          next_size > @max_encoded_bytes ->
            {:error, :encoded_value_too_large}

          status == :finished ->
            finalize_inflate(z, chunks, output)

          true ->
            inflate_bounded(z, <<>>, [output | chunks], next_size)
        end
    end
  end

  defp finalize_inflate(z, chunks, output) do
    case :zlib.inflateEnd(z) do
      :ok -> {:ok, chunks |> Enum.reverse([output]) |> IO.iodata_to_binary()}
      {:error, _reason} -> {:error, :invalid_wire_compression}
    end
  end

  defp encoded_size(value_binary) when is_binary(value_binary), do: byte_size(value_binary)
  defp encoded_size(_value_binary), do: nil
end
