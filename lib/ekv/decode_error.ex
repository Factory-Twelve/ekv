defmodule EKV.DecodeError do
  @moduledoc false

  defexception [:reason, :context, :encoded_size]

  @impl true
  def message(%__MODULE__{reason: reason, context: context, encoded_size: encoded_size}) do
    "EKV could not decode a stored value" <>
      " (context=#{inspect(context)}, reason=#{reason}, encoded_size=#{inspect(encoded_size)})"
  end
end
