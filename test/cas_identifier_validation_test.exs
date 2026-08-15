defmodule EKV.CASIdentifierValidationTest do
  use ExUnit.Case, async: false

  import Bitwise

  @max_node_id_bytes 1024
  @max_signed_64 9_223_372_036_854_775_807

  describe "node_id configuration" do
    test "rejects empty, oversized, and NUL-containing identifiers without CAS enabled" do
      for node_id <- ["", String.duplicate("n", @max_node_id_bytes + 1), <<"node", 0, "id">>] do
        name = unique_name("invalid_node_id")
        data_dir = temp_dir(name)
        previous_trap_exit = Process.flag(:trap_exit, true)

        assert {:error, {%ArgumentError{message: message}, _}} =
                 EKV.start_link(
                   name: name,
                   data_dir: data_dir,
                   node_id: node_id,
                   shards: 1,
                   log: false
                 )

        assert message =~ "without NUL bytes"

        Process.flag(:trap_exit, previous_trap_exit)
        File.rm_rf!(data_dir)
      end
    end

    test "preserves canonical binary identifiers at the maximum length" do
      node_id = String.duplicate("n", @max_node_id_bytes)
      name = unique_name("max_node_id")
      data_dir = temp_dir(name)

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          cluster_size: 1,
          node_id: node_id,
          shards: 1,
          log: false
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      assert EKV.Supervisor.get_config(name).node_id == node_id
    end
  end

  describe "Paxos NIF identifier validation" do
    setup do
      name = unique_name("paxos_identifiers")
      data_dir = temp_dir(name)

      {:ok, pid} =
        EKV.start_link(
          name: name,
          data_dir: data_dir,
          cluster_size: 1,
          node_id: "cas-nif",
          shards: 1,
          log: false,
          gc_interval: :timer.hours(1),
          tombstone_ttl: :timer.hours(24 * 7)
        )

      on_exit(fn ->
        Process.exit(pid, :shutdown)
        File.rm_rf!(data_dir)
      end)

      state = :sys.get_state(:"#{name}_ekv_replica_0")
      %{db: state.db, stmts: state.stmts}
    end

    test "rejects non-positive and non-int64 ballot counters", %{db: db, stmts: stmts} do
      value_args = value_args()

      for ballot_c <- [0, -1, @max_signed_64, 1 <<< 80] do
        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_prepare(db, "counter/prepare", ballot_c, "node-a")
        end

        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_accept(db, "counter/accept", ballot_c, "node-a", value_args)
        end

        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_promote(
            db,
            stmts.kv_force_upsert,
            stmts.keyref_upsert,
            stmts.oplog_insert,
            "counter/promote",
            ballot_c,
            "node-a"
          )
        end
      end
    end

    test "rejects empty, oversized, and NUL-containing ballot node identifiers", %{
      db: db,
      stmts: stmts
    } do
      value_args = value_args()

      for ballot_n <- [
            "",
            ["node", "-a"],
            String.duplicate("n", @max_node_id_bytes + 1),
            <<"node", 0, "a">>
          ] do
        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_prepare(db, "node/prepare", 1, ballot_n)
        end

        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_accept(db, "node/accept", 1, ballot_n, value_args)
        end

        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_promote(
            db,
            stmts.kv_force_upsert,
            stmts.keyref_upsert,
            stmts.oplog_insert,
            "node/promote",
            1,
            ballot_n
          )
        end
      end
    end

    test "uses length-aware ballot ordering and preserves the empty stored sentinel", %{db: db} do
      max_node_id = String.duplicate("n", @max_node_id_bytes)

      assert {:ok, :promise, 0, "", nil} =
               EKV.Store.paxos_prepare(db, "ordering/max", 1, max_node_id)

      assert {:ok, :promise, 0, "", nil} =
               EKV.Store.paxos_prepare(db, "ordering/prefix", 1, "node")

      assert {:ok, :promise, 0, "", nil} =
               EKV.Store.paxos_prepare(db, "ordering/prefix", 1, "node-a")

      assert {:ok, :nack, 1, "node-a"} =
               EKV.Store.paxos_prepare(db, "ordering/prefix", 1, "node")
    end

    test "rejects malformed accept lists and unsafe explicit promotion sequences", %{
      db: db,
      stmts: stmts
    } do
      assert_raise ArgumentError, fn ->
        EKV.Store.paxos_accept(db, "args/accept", 1, "node-a", value_args() ++ [:extra])
      end

      for origin_seq <- [-1, @max_signed_64] do
        assert_raise ArgumentError, fn ->
          EKV.Store.paxos_promote(
            db,
            stmts.kv_force_upsert,
            stmts.keyref_upsert,
            stmts.oplog_insert,
            "args/promote",
            1,
            "node-a",
            origin_seq
          )
        end
      end
    end

    test "rejects INT64_MAX promised counters persisted before prepare", %{db: db} do
      key = "persisted/prepare"
      seed_paxos_ballots(db, key, @max_signed_64, "node-a", 0, "")

      assert {:error, ~c"invalid persisted Paxos ballot"} =
               EKV.Store.paxos_prepare(db, key, 1, "node-b")
    end

    test "rejects INT64_MAX promised counters persisted before accept", %{db: db} do
      key = "persisted/accept"
      seed_paxos_ballots(db, key, @max_signed_64, "node-a", 0, "")

      assert {:error, ~c"invalid persisted Paxos ballot"} =
               EKV.Store.paxos_accept(db, key, 1, "node-b", value_args())
    end

    test "rejects INT64_MAX accepted counters persisted before promote", %{
      db: db,
      stmts: stmts
    } do
      key = "persisted/promote"
      seed_paxos_ballots(db, key, 1, "node-a", @max_signed_64, "node-a")

      assert {:error, ~c"invalid persisted Paxos ballot"} =
               EKV.Store.paxos_promote(
                 db,
                 stmts.kv_force_upsert,
                 stmts.keyref_upsert,
                 stmts.oplog_insert,
                 key,
                 1,
                 "node-a"
               )
    end
  end

  defp seed_paxos_ballots(
         db,
         key,
         promised_counter,
         promised_node,
         accepted_counter,
         accepted_node
       ) do
    {:ok, stmt} =
      EKV.Sqlite3.prepare(db, """
      INSERT INTO kv_paxos (
        key,
        promised_counter,
        promised_node,
        accepted_counter,
        accepted_node
      ) VALUES (?1, ?2, ?3, ?4, ?5)
      """)

    :ok =
      EKV.Sqlite3.bind(stmt, [
        key,
        promised_counter,
        promised_node,
        accepted_counter,
        accepted_node
      ])

    :done = EKV.Sqlite3.step(db, stmt)
    :ok = EKV.Sqlite3.release(db, stmt)
  end

  defp value_args do
    [:erlang.term_to_binary("value"), System.system_time(:nanosecond), "cas-nif", nil, nil]
  end

  defp unique_name(prefix) do
    :"ekv_#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp temp_dir(name), do: Path.join(System.tmp_dir!(), Atom.to_string(name))
end
