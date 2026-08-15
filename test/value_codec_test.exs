defmodule EKV.ValueCodecTest do
  use ExUnit.Case, async: false

  alias EKV.ValueCodec

  test "safe decode does not create atoms from poisoned ETF" do
    assert {:ok, :ok} = ValueCodec.decode(:erlang.term_to_binary(:ok))
    poisoned = external_atom("ekv_poison_#{System.unique_integer([:positive])}")
    atom_count = :erlang.system_info(:atom_count)

    assert {:error, :invalid_or_unsafe_external_term} = ValueCodec.decode(poisoned)
    assert :erlang.system_info(:atom_count) == atom_count
  end

  test "rejects malformed, compressed, trailing, and oversized ETF deterministically" do
    assert {:error, :invalid_or_unsafe_external_term} = ValueCodec.decode(<<131, 104>>)

    compressed = :erlang.term_to_binary(String.duplicate("compressed", 1_000), compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed
    assert {:error, :compressed_external_term} = ValueCodec.decode(compressed)

    trailing = :erlang.term_to_binary(%{safe: true}) <> <<0, 1>>
    assert {:error, :trailing_bytes} = ValueCodec.decode(trailing)

    oversized = :binary.copy(<<0>>, ValueCodec.max_encoded_bytes() + 1)
    assert {:error, :encoded_value_too_large} = ValueCodec.decode(oversized)

    oversized_value = :binary.copy(<<0>>, ValueCodec.max_encoded_bytes())
    assert {:error, :encoded_value_too_large} = ValueCodec.encode(oversized_value)
  end

  test "decoded heap growth is bounded" do
    element_count = div(ValueCodec.max_decoded_heap_words(), 2) + 100_000

    encoded_list =
      <<131, 108, element_count::unsigned-big-32>> <>
        :binary.copy(<<97, 0>>, element_count) <> <<106>>

    assert {:error, :decoded_value_too_large} = ValueCodec.decode(encoded_list)
  end

  test "wire decompression is bounded and rejects corrupt zlib payloads" do
    value_binary = :erlang.term_to_binary(%{safe: String.duplicate("value", 1_000)})
    compressed = :zlib.compress(value_binary)
    assert {:ok, ^value_binary} = ValueCodec.decompress_wire(compressed)
    assert {:error, :invalid_wire_compression} = ValueCodec.decompress_wire(<<1, 2, 3>>)

    for removed_bytes <- 1..5 do
      truncated = binary_part(compressed, 0, byte_size(compressed) - removed_bytes)

      assert {:error, :invalid_wire_compression} =
               ValueCodec.decompress_wire(truncated)
    end
  end

  defp external_atom(name) when byte_size(name) < 256 do
    <<131, 119, byte_size(name), name::binary>>
  end
end
