(** Integration tests for kafka-eio-consumer against a local Redpanda broker.
    Seeds test messages via kafka-eio-producer, then consumes and verifies them.
    Override broker location with the standard Kafka broker environment variable. *)

let test_topic = "sun-consumer-test"

let seed_messages sw n =
  let cfg = Kafka_test_helpers.default_producer_config () in
  match Kafka.Producer.create cfg ~sw with
  | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
  | Ok producer ->
    let promises = List.init n (fun i ->
      Kafka.Producer.produce_await producer
        ~topic:test_topic
        ~key:(Bytes.of_string (string_of_int i))
        ~value:(Some (Bytes.of_string (Printf.sprintf "message-%04d" i))) ()
    ) in
    List.iter (fun p ->
      match Eio.Promise.await p with
      | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
      | Ok () -> ()
    ) promises;
    Kafka.Producer.close producer

let make_consumer_config () : Kafka.Consumer.config =
  Kafka_test_helpers.default_consumer_config
    ~group_id:(Printf.sprintf "sun-test-%d" (Unix.getpid ()))
    ~topics:[ test_topic ]
    ()

let test_poll_messages () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      seed_messages sw 5;

      match Kafka.Consumer.create (make_consumer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let received = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
          while List.length !received < 5 do
            let msg =
              match Kafka.Consumer.fetch consumer with
              | Ok msg -> msg
              | Error e -> Alcotest.failf "fetch failed: %s" (Kafka.Error.to_string e)
            in
            received := msg :: !received;
            ignore (Kafka.Consumer.commit consumer msg)
          done;
          Ok ()
        ) with
        | Error `Timeout -> Alcotest.fail "timed out waiting for messages"
        | Ok () -> ());
        Alcotest.(check int) "received 5 messages" 5 (List.length !received);
        Kafka.Consumer.close consumer

let test_consume_with_ack () =
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      seed_messages sw 3;

      match Kafka.Consumer.create (make_consumer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let count = ref 0 in
        let _ =
          Kafka.Consumer.consume consumer ~handler:(fun _msg ~ack ->
            ignore (ack ());
            incr count;
            if !count >= 3 then Kafka.Consumer.Stop
            else Kafka.Consumer.Continue
          ) ()
        in
        Alcotest.(check int) "consumed 3 messages" 3 !count;
        Kafka.Consumer.close consumer

let test_fetch_api () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      seed_messages sw 4;

      match Kafka.Consumer.create (make_consumer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let msgs = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
          for _ = 1 to 4 do
            let msg =
              match Kafka.Consumer.fetch consumer with
              | Ok msg -> msg
              | Error e -> Alcotest.failf "fetch failed: %s" (Kafka.Error.to_string e)
            in
            msgs := msg :: !msgs
          done;
          Ok ()
        ) with
        | Error `Timeout -> Alcotest.fail "timed out waiting for fetched messages"
        | Ok () -> ());
        Alcotest.(check int) "fetch returned 4 messages" 4 (List.length !msgs);
        Kafka.Consumer.close consumer

(* A NULL payload (tombstone) must stay distinct from a genuine
   zero-length value at the consumer FFI boundary. Uses its own
   topic/group since the other tests here share test_topic with exact
   message-count assertions. *)
let test_tombstone () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-tombstone-%d" pid in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         List.iter (fun value ->
           match Eio.Promise.await (Kafka.Producer.produce_await producer ~topic ~value ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
           | Ok () -> ()
         ) [ Some (Bytes.of_string "before"); None; Some Bytes.empty; Some (Bytes.of_string "after") ];
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-tombstone-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let values = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           while List.length !values < 4 do
             match Kafka.Consumer.fetch consumer with
             | Ok msg -> values := msg.value :: !values
             | Error e -> Alcotest.failf "fetch failed: %s" (Kafka.Error.to_string e)
           done;
           Ok ()
         ) with
         | Error `Timeout -> Alcotest.fail "timed out waiting for messages"
         | Ok () -> ());
        let values = List.rev_map (Option.map Bytes.to_string) !values in
        Alcotest.(check (list (option string)))
          "tombstone (None) stays distinct from a real value, including a real empty one"
          [ Some "before"; None; Some ""; Some "after" ]
          values;
        Kafka.Consumer.close consumer

(* A producer-sent NULL-valued header must stay distinct from Some "",
   mirroring the consumer read side. *)
let test_header_with_null_value () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-headers-%d" pid in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         (match Eio.Promise.await (Kafka.Producer.produce_await producer
                  ~topic ~value:(Some (Bytes.of_string "with-headers"))
                  ~headers:[ ("present", Some "v1"); ("absent", None) ] ()) with
          | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-headers-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let msg = ref None in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           match Kafka.Consumer.fetch consumer with
           | Ok m -> msg := Some m; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka.Error.to_string e))
         ) with
         | Error `Timeout -> Alcotest.fail "timed out waiting for message"
         | Error (`Fetch_failed m) -> Alcotest.failf "fetch failed: %s" m
         | Ok () -> ());
        let msg = Option.get !msg in
        Alcotest.(check (list (pair string (option string))))
          "NULL-valued header stays distinct from Some \"\""
          [ ("present", Some "v1"); ("absent", None) ]
          msg.headers;
        Kafka.Consumer.close consumer

(* A non-NULL msg->key with key_len 0 is a genuine zero-length key,
   distinct from NULL ("no key") — must stay that way through the
   consumer FFI decode too. *)
let test_zero_length_key_distinct_from_no_key () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-keys-%d" pid in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         let send ?key value =
           match Eio.Promise.await (Kafka.Producer.produce_await producer ~topic ~value ?key ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
           | Ok () -> ()
         in
         send ~key:Bytes.empty (Some (Bytes.of_string "zero-length-key"));
         send (Some (Bytes.of_string "no-key"));
         send ~key:(Bytes.of_string "k") (Some (Bytes.of_string "real-key"));
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-keys-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let keys = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           while List.length !keys < 3 do
             match Kafka.Consumer.fetch consumer with
             | Ok msg -> keys := msg.key :: !keys
             | Error e -> Alcotest.failf "fetch failed: %s" (Kafka.Error.to_string e)
           done;
           Ok ()
         ) with
         | Error `Timeout -> Alcotest.fail "timed out waiting for messages"
         | Ok () -> ());
        let keys = List.rev_map (Option.map Bytes.to_string) !keys in
        Alcotest.(check (list (option string)))
          "zero-length key stays distinct from no key"
          [ Some ""; None; Some "k" ]
          keys;
        Kafka.Consumer.close consumer

(* A partition fiber that exits without draining its own queue — on
   Stop, exhausted retries, or being interrupted mid-retry-sleep by
   another partition's Stop — left that queue full with nobody to drain
   it, hanging routing_loop's (or the shutdown sentinel's) blocking
   Eio.Stream.add forever.

   Routing order across partitions isn't controllable here, so the
   "stuck" partition self-exhausts via bounded retries regardless of
   whether the other partition's Stop wins first; either way the call
   must still return rather than hang, and the timeout is a safety net
   so a regression fails cleanly instead of wedging the suite. *)
let test_consume_partitioned_stop_does_not_hang () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-partitioned-stop-%d" pid in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:2 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         for i = 0 to 9 do
           match Eio.Promise.await (Kafka.Producer.produce_await producer
                   ~topic ~key:(Bytes.of_string (string_of_int i))
                   ~value:(Some (Bytes.of_string (string_of_int i))) ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
           | Ok () -> ()
         done;
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-partitioned-stop-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let stuck_partition = ref None in
        let handler (msg : Kafka.Consumer.message) ~ack =
          match !stuck_partition with
          | None ->
            stuck_partition := Some msg.partition;
            Kafka.Consumer.Error "stuck"
          | Some p when Int32.equal p msg.partition ->
            Kafka.Consumer.Error "stuck"
          | Some _ ->
            ignore (ack ());
            Kafka.Consumer.Stop
        in
        let retry : Kafka.Consumer.retry_policy =
          { base_delay_s = 1.0; max_delay_s = 1.0; max_attempts = 3 }
        in
        let result =
          Eio.Time.with_timeout env#clock 20.0 (fun () ->
            Ok (Kafka.Consumer.consume_partitioned consumer ~sw ~clock:env#clock
                  ~retry ~queue_capacity:3 ~handler ()))
        in
        (* Either termination is a legitimate, non-hanging outcome here:
           Ok () if the other partition's Stop won the race, Handler_errors
           if the stuck partition self-exhausted first. What matters is that
           the call returns at all instead of hanging. *)
        (match result with
         | Error `Timeout ->
           Alcotest.fail "consume_partitioned hung — a stopped partition fiber \
                          left its queue abandoned-but-full"
         | Ok (Ok ()) -> ()
         | Ok (Error (Kafka.Consumer.Handler_errors [ (_partition, "stuck") ])) -> ()
         | Ok (Error (Kafka.Consumer.Handler_errors errors)) ->
           Alcotest.failf "unexpected handler error count: %d" (List.length errors)
         | Ok (Error (Kafka.Consumer.Invalid_config e)) ->
           Alcotest.failf "unexpected invalid config: %s" e);
        Kafka.Consumer.close consumer

(* librdkafka's default auto.offset.store advances the "stored" position
   on every message poll_fiber prefetches, whether processed or not —
   commit_all (which commits that position for a NULL partition list)
   could silently commit past messages never actually processed. Process
   only the first of 5 seeded messages, commit_all, then confirm a fresh
   consumer in the group still receives message 1. *)
let test_commit_all_does_not_commit_past_processed () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-commit-all-%d" pid in
      let group_id = Printf.sprintf "kafka-eio-test-commit-all-group-%d" pid in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         for i = 0 to 4 do
           match Eio.Promise.await (Kafka.Producer.produce_await producer
                   ~topic ~value:(Some (Bytes.of_string (string_of_int i))) ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
           | Ok () -> ()
         done;
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      (match Kafka.Consumer.create cfg ~sw with
       | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
       | Ok consumer ->
         (* Let poll_fiber prefetch the whole backlog before processing
            just one message — the precondition for the stored position
            to drift ahead of what was processed. *)
         Eio.Time.sleep env#clock 1.0;
         (match Kafka.Consumer.fetch consumer with
          | Error e -> Alcotest.failf "fetch failed: %s" (Kafka.Error.to_string e)
          | Ok msg ->
            (match Kafka.Consumer.commit consumer msg with
             | Error e -> Alcotest.failf "commit failed: %s" (Kafka.Error.to_string e)
             | Ok () -> ()));
         (match Kafka.Consumer.commit_all consumer with
          | Error e -> Alcotest.failf "commit_all failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         Kafka.Consumer.close consumer);

      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "second consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer2 ->
        let next = ref None in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           match Kafka.Consumer.fetch consumer2 with
           | Ok msg -> next := Some msg; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka.Error.to_string e))
         ) with
         | Error `Timeout ->
           Alcotest.fail "no message left after commit_all — it committed past \
                          the one message actually processed"
         | Error (`Fetch_failed m) -> Alcotest.failf "fetch failed: %s" m
         | Ok () -> ());
        let next = Option.get !next in
        Alcotest.(check (option string)) "commit_all did not commit past what was processed"
          (Some "1") (Option.map Bytes.to_string next.value);
        Kafka.Consumer.close consumer2

(* Closing the consumer directly (instead of cancelling ~sw) never
   resolves consume_partitioned's internal stop_p, so routing_loop's
   Fiber.first would block forever once poll_fiber stops feeding
   t.stream. The handler here always returns Continue, so the only way
   this call can return is via the close-triggered watchdog. *)
let test_consume_partitioned_stops_on_direct_close () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-partitioned-close-%d" pid in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         (match Eio.Promise.await (Kafka.Producer.produce_await producer
                  ~topic ~value:(Some (Bytes.of_string "x")) ()) with
          | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-partitioned-close-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let handler _msg ~ack = ignore (ack ()); Kafka.Consumer.Continue in
        let result =
          Eio.Time.with_timeout env#clock 10.0 (fun () ->
            Ok (Eio.Fiber.first
              (fun () ->
                 Kafka.Consumer.consume_partitioned consumer ~sw ~clock:env#clock ~handler ())
              (fun () ->
                 Eio.Time.sleep env#clock 1.0;
                 Kafka.Consumer.close consumer;
                 Eio.Time.sleep env#clock 30.0;
                 Ok ())))
        in
        (match result with
         | Error `Timeout ->
           Alcotest.fail "consume_partitioned hung after close was called directly"
         | Ok (Ok ()) -> ()
         | Ok (Error _) -> Alcotest.fail "unexpected handler error")

(* last_processed (commit_all's source of truth when auto_commit = false)
   was never invalidated across a rebalance — a partition revoked and
   later reassigned back could still have its stale, pre-revoke offset
   committed by commit_all, rolling the group's committed position
   backward past what a second consumer processed on it meanwhile.

   c1 owns both partitions and acks everything; c2 joins (triggering an
   eager-rebalance split) and both drain fresh data; c2 leaves, handing
   its partition(s) back to c1, which calls commit_all having fetched
   nothing new on the reclaimed partition. A fresh consumer afterward
   must not re-receive anything c2 already committed. *)
let test_commit_all_survives_rebalance () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-rebalance-%d" pid in
      let group_id = Printf.sprintf "kafka-eio-test-rebalance-group-%d" pid in
      let produce_range producer lo hi =
        for i = lo to hi do
          match Eio.Promise.await (Kafka.Producer.produce_await producer
                  ~topic ~key:(Bytes.of_string (string_of_int i))
                  ~value:(Some (Bytes.of_string (Printf.sprintf "seed-%d" i))) ()) with
          | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ()
        done
      in
      (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka.Error.to_string e)
       | Ok producer ->
         (match Kafka.Producer.create_topic producer
                  ~topic_name:topic ~partitions:2 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka.Error.to_string e)
          | Ok () -> ());
         produce_range producer 0 9;
         Kafka.Producer.close producer);

      let cfg : Kafka.Consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id;
        topics       = [ topic ];
        offset_reset = Kafka.Consumer.Earliest;
        auto_commit  = false;
        security     = Kafka.Security.default;
        properties   = [];
      } in
      let drain_and_ack c ~budget_s =
        let deadline = Unix.gettimeofday () +. budget_s in
        let quiet_deadline = ref None in
        let rec loop () =
          if Unix.gettimeofday () > deadline then ()
          else
            match Kafka.Consumer.poll c with
            | Error e -> Alcotest.failf "poll failed: %s" (Kafka.Error.to_string e)
            | Ok None ->
              let now = Unix.gettimeofday () in
              (match !quiet_deadline with
               | Some t when now >= t -> ()
               | None -> Eio.Time.sleep env#clock 0.1; loop ()
               | Some _ -> Eio.Time.sleep env#clock 0.1; loop ())
            | Ok (Some msg) ->
              quiet_deadline := Some (Unix.gettimeofday () +. 0.5);
              (match Kafka.Consumer.commit c msg with
               | Error e -> Alcotest.failf "commit failed: %s" (Kafka.Error.to_string e)
               | Ok () -> ());
              loop ()
        in
        loop ()
      in
      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "c1 create failed: %s" (Kafka.Error.to_string e)
      | Ok c1 ->
        drain_and_ack c1 ~budget_s:5.0;

        (* c2 joins the same group — triggers a rebalance splitting the 2
           partitions between c1 and c2. *)
        Eio.Switch.run (fun sw2 ->
          let c2_ready, c2_ready_r = Eio.Promise.create () in
          match Kafka.Consumer.create cfg ~sw:sw2 ~on_ready:(fun () ->
                  Eio.Promise.resolve c2_ready_r ()) with
          | Error e -> Alcotest.failf "c2 create failed: %s" (Kafka.Error.to_string e)
          | Ok c2 ->
            (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
               Eio.Promise.await c2_ready; Ok ()) with
             | Ok () -> ()
             | Error `Timeout -> Alcotest.fail "timed out waiting for c2 assignment");
            (match Kafka.Producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
             | Error e -> Alcotest.failf "second seed producer create failed: %s" (Kafka.Error.to_string e)
             | Ok producer2 -> produce_range producer2 10 29; Kafka.Producer.close producer2);
            drain_and_ack c1 ~budget_s:5.0;
            drain_and_ack c2 ~budget_s:5.0
          (* c2's switch ends here — closing it triggers another
             rebalance, handing its partition(s) back to c1. *));

        Eio.Time.sleep env#clock 3.0; (* let that rebalance settle too *)
        (match Kafka.Consumer.commit_all c1 with
         | Error e -> Alcotest.failf "c1 commit_all failed: %s" (Kafka.Error.to_string e)
         | Ok () -> ());
        Kafka.Consumer.close c1;

      match Kafka.Consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "c3 create failed: %s" (Kafka.Error.to_string e)
      | Ok c3 ->
        let unexpected = ref None in
        (match Eio.Time.with_timeout env#clock 2.0 (fun () ->
           match Kafka.Consumer.fetch c3 with
           | Ok msg -> unexpected := Some msg; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka.Error.to_string e))
         ) with
         | Error `Timeout -> () (* expected: nothing left to redeliver *)
         | Error (`Fetch_failed m) -> Alcotest.failf "fetch failed: %s" m
         | Ok () ->
           let msg = Option.get !unexpected in
           Alcotest.failf
             "commit_all rolled back the group's committed offset — \
              re-received topic=%s partition=%ld offset=%Ld"
             msg.topic msg.partition msg.offset);
        Kafka.Consumer.close c3

let () =
  let open Alcotest in
  run "kafka_consumer_integration" [
    "consume", [
      test_case "poll messages"    `Slow test_poll_messages;
      test_case "consume with ack" `Slow test_consume_with_ack;
      test_case "fetch api"        `Slow test_fetch_api;
      test_case "tombstone stays distinct from empty value" `Slow test_tombstone;
      test_case "header with null value stays distinct from empty string" `Slow
        test_header_with_null_value;
      test_case "zero-length key stays distinct from no key" `Slow
        test_zero_length_key_distinct_from_no_key;
      test_case "consume_partitioned stop does not hang" `Slow
        test_consume_partitioned_stop_does_not_hang;
      test_case "commit_all does not commit past what was processed" `Slow
        test_commit_all_does_not_commit_past_processed;
      test_case "consume_partitioned stops on direct close" `Slow
        test_consume_partitioned_stops_on_direct_close;
      test_case "commit_all survives a rebalance without rolling back" `Slow
        test_commit_all_survives_rebalance;
    ];
  ]
