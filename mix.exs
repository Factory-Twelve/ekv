defmodule EKV.MixProject do
  use Mix.Project

  @version "0.4.3"

  def project do
    [
      app: :ekv,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      description: description(),
      package: package(),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {EKV.Application, []}
    ]
  end

  defp description do
    """
    Eventually consistent durable KV store for Elixir with zero runtime dependencies.
    Data survives node restarts, node death, and network partitions.
    Direct member replication across Erlang nodes with delta sync.
    """
  end

  defp package do
    [
      name: "ekv",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/Factory-Twelve/ekv"},
      files: [
        "lib",
        "c_src/ekv_sqlite3_nif.c",
        "c_src/sqlite3.c",
        "c_src/sqlite3.h",
        "Makefile",
        "mix.exs",
        "README.md",
        "LICENSE.md"
      ]
    ]
  end

  defp deps do
    [
      {:elixir_make, "== 0.9.0", runtime: false},
      {:ex_doc, "~> 0.38", only: :docs}
    ]
  end
end
