(** Integration tests for kafka-eio-producer against a local Redpanda broker.
    Requires: rpk redpanda start (or equivalent) before running.
    Override broker location with the standard Kafka broker environment variable. *)

let test_topic = "sun-producer-test"

let test_produce_fire_and_forget () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        (match Kafka_producer.produce producer ~topic:test_topic
                 ~value:(Some (Bytes.of_string "hello-fire-and-forget")) () with
         | Error e -> Alcotest.failf "produce failed: %s" (Kafka_error.to_string e)
         | Ok () ->
           match Kafka_producer.flush producer ~timeout_ms:5000 with
           | Error e -> Alcotest.failf "flush failed: %s" (Kafka_error.to_string e)
           | Ok ()   -> ());
        Kafka_producer.close producer

let test_produce_await () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        let promise =
          Kafka_producer.produce_await producer
            ~topic:test_topic
            ~value:(Some (Bytes.of_string "hello-awaited")) ()
        in
        (match Eio.Promise.await promise with
         | Error e -> Alcotest.failf "produce_await failed: %s" (Kafka_error.to_string e)
         | Ok ()   -> ());
        Kafka_producer.close producer

let test_produce_with_key () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        let promise =
          Kafka_producer.produce_await producer
            ~topic:test_topic
            ~value:(Some (Bytes.of_string "hello-keyed"))
            ~key:(Bytes.of_string "my-key") ()
        in
        (match Eio.Promise.await promise with
         | Error e -> Alcotest.failf "produce_await with key failed: %s" (Kafka_error.to_string e)
         | Ok ()   -> ());
        Kafka_producer.close producer

let test_produce_many () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        let promises = List.init 100 (fun i ->
          Kafka_producer.produce_await producer
            ~topic:test_topic
            ~value:(Some (Bytes.of_string (Printf.sprintf "msg-%04d" i))) ()
        ) in
        List.iter (fun p ->
          match Eio.Promise.await p with
          | Error e -> Alcotest.failf "batch produce_await failed: %s" (Kafka_error.to_string e)
          | Ok ()   -> ()
        ) promises;
        Kafka_producer.close producer

(* with_transaction must abort on an exception raised inside f, not just
   on Error, or the transaction stays open and later transactional calls
   fail with a state/concurrent-transaction error. *)
let test_with_transaction_aborts_on_exception () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->
      let cfg : Kafka_producer.config = {
        brokers       = Kafka_test_helpers.brokers ();
        delivery_mode = Kafka_producer.Exactly_once
                          { transaction_id = Printf.sprintf "test-txn-abort-%d" (Unix.getpid ()) };
        linger_ms     = None;
        security      = Kafka_security.default;
        properties    = [];
      } in
      match Kafka_producer.create cfg ~sw with
      | Error e -> Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        (match
           (try
              ignore (Kafka_producer.with_transaction producer (fun () ->
                ignore (Kafka_producer.produce producer ~topic:test_topic
                          ~value:(Some (Bytes.of_string "should-be-aborted")) ());
                failwith "boom"));
              Ok ()
            with Failure msg -> Error msg)
         with
         | Ok () -> Alcotest.fail "expected the exception to propagate"
         | Error msg -> Alcotest.(check bool) "propagated exception" true (msg = "boom"));
        (* The prior transaction must have been aborted, not left open —
           otherwise this second transaction fails immediately. *)
        (match Kafka_producer.with_transaction producer (fun () -> Ok ()) with
         | Ok ()   -> ()
         | Error e ->
           Alcotest.failf
             "second transaction failed — first transaction was likely left open: %s"
             (Kafka_producer.string_of_transaction_error e));
        Kafka_producer.close producer

(* send_offsets_to_transaction must commit exactly the offsets the caller
   says it processed, not the consumer's current position. Seeds two
   messages, processes only the first inside a transaction, then confirms
   the second is still delivered to a fresh consumer in the same group. *)
let test_transaction_commits_only_processed_offset () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let input_topic  = Printf.sprintf "kafka-eio-test-txn-input-%d" pid in
      let output_topic = Printf.sprintf "kafka-eio-test-txn-output-%d" pid in
      let group_id     = Printf.sprintf "kafka-eio-test-txn-group-%d" pid in

      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "setup producer create failed: %s" (Kafka_error.to_string e)
       | Ok setup_producer ->
         List.iter (fun topic ->
           match Kafka_producer.create_topic setup_producer
                   ~topic_name:topic ~partitions:1 ~replication_factor:1 with
           | Error e -> Alcotest.failf "create_topic %s failed: %s" topic (Kafka_error.to_string e)
           | Ok () -> ()
         ) [ input_topic; output_topic ];
         List.iter (fun i ->
           match Eio.Promise.await (Kafka_producer.produce_await setup_producer
             ~topic:input_topic ~value:(Some (Bytes.of_string (Printf.sprintf "in-%d" i))) ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
           | Ok () -> ()
         ) [ 0; 1 ];
         Kafka_producer.close setup_producer);

      let consumer_cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id;
        topics       = [ input_topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      match Kafka_consumer.create consumer_cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let first_msg = ref None in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           match Kafka_consumer.fetch consumer with
           | Ok msg -> first_msg := Some msg; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka_error.to_string e))
         ) with
         | Error `Timeout -> Alcotest.fail "timed out waiting for first message"
         | Error (`Fetch_failed msg) -> Alcotest.failf "fetch failed: %s" msg
         | Ok () -> ());
        let first_msg = Option.get !first_msg in

        let txn_cfg : Kafka_producer.config = {
          brokers       = Kafka_test_helpers.brokers ();
          delivery_mode = Kafka_producer.Exactly_once
                            { transaction_id = Printf.sprintf "kafka-eio-test-txn-offsets-%d" pid };
          linger_ms     = None;
          security      = Kafka_security.default;
          properties    = [];
        } in
        (match Kafka_producer.create txn_cfg ~sw with
         | Error e -> Alcotest.failf "txn producer create failed: %s" (Kafka_error.to_string e)
         | Ok txn_producer ->
           (match Kafka_producer.with_transaction txn_producer
                    ~consumer_offsets:(Kafka_consumer.handle consumer,
                                       [ (first_msg.topic, first_msg.partition, first_msg.offset) ])
                    (fun () ->
                       Kafka_producer.produce txn_producer ~topic:output_topic
                         ~value:(Some (Bytes.of_string "out-0")) ())
            with
            | Error e ->
              Alcotest.failf "transaction failed: %s" (Kafka_producer.string_of_transaction_error e)
            | Ok () -> ());
           Kafka_producer.close txn_producer);
        Kafka_consumer.close consumer;

        Eio.Switch.run (fun sw2 ->
          match Kafka_consumer.create consumer_cfg ~sw:sw2 with
          | Error e -> Alcotest.failf "second consumer create failed: %s" (Kafka_error.to_string e)
          | Ok consumer2 ->
            let second_msg = ref None in
            (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
               match Kafka_consumer.fetch consumer2 with
               | Ok msg -> second_msg := Some msg; Ok ()
               | Error e -> Error (`Fetch_failed (Kafka_error.to_string e))
             ) with
             | Error `Timeout ->
               Alcotest.fail "timed out waiting for second message — the transaction likely \
                              committed past the unprocessed message"
             | Error (`Fetch_failed msg) -> Alcotest.failf "fetch failed: %s" msg
             | Ok () -> ());
            let second_msg = Option.get !second_msg in
            Alcotest.(check (option string)) "second message is the one not processed"
              (Some "in-1") (Option.map Bytes.to_string second_msg.value);
            Kafka_consumer.close consumer2)

(* Guards against close leaking the delivery/wake pipes when
   delivery_fiber has an actual blocked read to cancel (not just an idle
   producer). *)
let test_close_does_not_leak_fds_with_real_deliveries () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->
      let fd_count () = Array.length (Sys.readdir "/proc/self/fd") in
      let before = fd_count () in
      for _ = 1 to 5 do
        match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
        | Error e -> Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
        | Ok producer ->
          (match Eio.Promise.await (Kafka_producer.produce_await producer
                   ~topic:test_topic ~value:(Some (Bytes.of_string "fd-leak-check")) ()) with
           | Error e -> Alcotest.failf "produce_await failed: %s" (Kafka_error.to_string e)
           | Ok () -> ());
          Kafka_producer.close producer
      done;
      let after = fd_count () in
      Alcotest.(check bool)
        (Printf.sprintf "fd count stable across 5 create/produce/close cycles (before=%d after=%d)"
           before after)
        true (after <= before + 2)

let () =
  let open Alcotest in
  run "kafka_producer_integration" [
    "produce", [
      test_case "fire-and-forget" `Slow test_produce_fire_and_forget;
      test_case "produce_await"   `Slow test_produce_await;
      test_case "with key"        `Slow test_produce_with_key;
      test_case "100 messages"    `Slow test_produce_many;
      test_case "close does not leak fds with real deliveries" `Slow
        test_close_does_not_leak_fds_with_real_deliveries;
    ];
    "transactions", [
      test_case "with_transaction aborts on exception" `Slow
        test_with_transaction_aborts_on_exception;
      test_case "transaction commits only the processed offset" `Slow
        test_transaction_commits_only_processed_offset;
    ];
  ]
