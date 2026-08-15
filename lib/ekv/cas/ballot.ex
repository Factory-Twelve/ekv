defmodule EKV.CAS.Ballot do
  @moduledoc false

  alias EKV.WireEnvelope

  @max_signed_64 9_223_372_036_854_775_807
  @max_counter @max_signed_64 - 1
  @max_future_skew_ns :timer.minutes(5) * 1_000_000

  def max_counter, do: @max_counter

  def valid?(counter, node_id) do
    is_integer(counter) and counter > 0 and counter <= @max_counter and valid_node_id?(node_id)
  end

  def valid_incoming?(counter, node_id, now_ns \\ System.system_time(:nanosecond)) do
    valid?(counter, node_id) and counter <= now_ns + @max_future_skew_ns
  end

  def valid_node_id?(node_id) do
    is_binary(node_id) and byte_size(node_id) > 0 and
      byte_size(node_id) <= WireEnvelope.max_origin_bytes() and
      :binary.match(node_id, <<0>>) == :nomatch
  end

  def valid_accepted?(0, "", _proposed), do: true

  def valid_accepted?(counter, node_id, {proposed_counter, proposed_node_id}) do
    valid?(counter, node_id) and {counter, node_id} < {proposed_counter, proposed_node_id}
  end

  def valid_nack?(0, "", _proposed), do: true

  def valid_nack?(counter, node_id, {proposed_counter, proposed_node_id}) do
    valid_incoming?(counter, node_id) and
      {counter, node_id} >= {proposed_counter, proposed_node_id}
  end

  def next(local_counter, node_id, now_ns \\ System.system_time(:nanosecond)) do
    counter = max(now_ns, local_counter + 1)

    if valid_incoming?(counter, node_id, now_ns) do
      {:ok, counter, node_id}
    else
      {:error, :counter_exhausted}
    end
  end

  def observe_nack(local_counter, promised_counter)
      when is_integer(local_counter) and is_integer(promised_counter) and promised_counter > 0 and
             promised_counter <= @max_counter do
    {:ok, max(local_counter, promised_counter)}
  end

  def observe_nack(_local_counter, _promised_counter), do: {:error, :invalid_promised_counter}
end
