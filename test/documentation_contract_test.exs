defmodule EKV.DocumentationContractTest do
  use ExUnit.Case, async: true

  test "documented encoded-value limits match the canonical wire-envelope limit" do
    documented_limit = formatted_integer(EKV.WireEnvelope.max_encoded_value_bytes())
    readme = File.read!(Path.expand("../README.md", __DIR__))

    {:docs_v1, _, :elixir, _, %{"en" => module_doc}, _, _} = Code.fetch_docs(EKV)

    assert readme =~ "uncompressed encoding is at most #{documented_limit} bytes"
    assert module_doc =~ "uncompressed encoding is at most #{documented_limit} bytes"
  end

  defp formatted_integer(integer) do
    integer
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end
end
