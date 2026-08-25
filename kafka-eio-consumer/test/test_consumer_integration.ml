(** Integration tests for kafka-eio-consumer against a local Redpanda broker.
    Seeds test messages via kafka-eio-producer, then consumes and verifies them.
    Override broker location with the standard Kafka broker environment variable. *)

let test_topic = "sun-consumer-test"

let seed_messages sw n =
  let cfg = Kafka_test_helpers.default_producer_config () in
  match Kafka_producer.create cfg ~sw with
  | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
  | Ok producer ->
    let promises = List.init n (fun i ->
      Kafka_producer.produce_await producer
        ~topic:test_topic
        ~key:(Bytes.of_string (string_of_int i))
        ~value:(Some (Bytes.of_string (Printf.sprintf "message-%04d" i))) ()
    ) in
    List.iter (fun p ->
      match Eio.Promise.await p with
      | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
      | Ok () -> ()
    ) promises;
    Kafka_producer.close producer

let make_consumer_config () : Kafka_consumer.config =
  Kafka_test_helpers.default_consumer_config
    ~group_id:(Printf.sprintf "sun-test-%d" (Unix.getpid ()))
    ~topics:[ test_topic ]
    ()

let test_poll_messages () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      seed_messages sw 5;

      match Kafka_consumer.create (make_consumer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let received = ref [] in
        let stream = Kafka_consumer.stream consumer in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
          while List.length !received < 5 do
            let msg = Eio.Stream.take stream in
            received := msg :: !received;
            ignore (Kafka_consumer.commit consumer msg)
          done;
          Ok ()
        ) with
        | Error `Timeout -> Alcotest.fail "timed out waiting for messages"
        | Ok () -> ());
        Alcotest.(check int) "received 5 messages" 5 (List.length !received);
        Kafka_consumer.close consumer

let test_consume_with_ack () =
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      seed_messages sw 3;

      match Kafka_consumer.create (make_consumer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let count = ref 0 in
        let _ =
          Kafka_consumer.consume consumer ~handler:(fun _msg ~ack ->
            ignore (ack ());
            incr count;
            if !count >= 3 then Kafka_consumer.Stop
            else Kafka_consumer.Continue
          ) ()
        in
        Alcotest.(check int) "consumed 3 messages" 3 !count;
        Kafka_consumer.close consumer

let test_stream_api () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      seed_messages sw 4;

      match Kafka_consumer.create (make_consumer_config ()) ~sw with
      | Error e ->
        Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let stream = Kafka_consumer.stream consumer in
        let msgs = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
          for _ = 1 to 4 do
            let msg = Eio.Stream.take stream in
            msgs := msg :: !msgs
          done;
          Ok ()
        ) with
        | Error `Timeout -> Alcotest.fail "timed out waiting for stream messages"
        | Ok () -> ());
        Alcotest.(check int) "stream yielded 4 messages" 4 (List.length !msgs);
        Kafka_consumer.close consumer

(* Regression note: a Kafka tombstone (NULL payload) must stay
   distinguishable from a genuine zero-length value at the consumer FFI
   boundary, and produce/produce_await must be able to send one at all.
   Uses its own topic/group rather than the shared test_topic above, since
   the other tests in this file share one consumer group and rely on
   exact message counts across sequential test cases. *)
let test_tombstone () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-tombstone-%d" pid in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         List.iter (fun value ->
           match Eio.Promise.await (Kafka_producer.produce_await producer ~topic ~value ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
           | Ok () -> ()
         ) [ Some (Bytes.of_string "before"); None; Some Bytes.empty; Some (Bytes.of_string "after") ];
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-tombstone-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let values = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           while List.length !values < 4 do
             match Kafka_consumer.fetch consumer with
             | Ok msg -> values := msg.value :: !values
             | Error e -> Alcotest.failf "fetch failed: %s" (Kafka_error.to_string e)
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
        Kafka_consumer.close consumer

(* Regression note: producer headers had no way to send a
   NULL-valued header, even though the consumer read side already
   preserved one as distinct from Some "" — this closes that gap. *)
let test_header_with_null_value () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-headers-%d" pid in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         (match Eio.Promise.await (Kafka_producer.produce_await producer
                  ~topic ~value:(Some (Bytes.of_string "with-headers"))
                  ~headers:[ ("present", Some "v1"); ("absent", None) ] ()) with
          | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-headers-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let msg = ref None in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           match Kafka_consumer.fetch consumer with
           | Ok m -> msg := Some m; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka_error.to_string e))
         ) with
         | Error `Timeout -> Alcotest.fail "timed out waiting for message"
         | Error (`Fetch_failed m) -> Alcotest.failf "fetch failed: %s" m
         | Ok () -> ());
        let msg = Option.get !msg in
        Alcotest.(check (list (pair string (option string))))
          "NULL-valued header stays distinct from Some \"\""
          [ ("present", Some "v1"); ("absent", None) ]
          msg.headers;
        Kafka_consumer.close consumer

(* Regression note: the consumer's C decode gated a key on
   [msg->key_len > 0] as well as non-NULL, collapsing a genuine
   zero-length key into "no key" — the same bug class already fixed for
   value/headers, missed on key. librdkafka gives a non-NULL msg->key
   with key_len 0 for a real zero-length key, NULL only for no key. *)
let test_zero_length_key_distinct_from_no_key () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-keys-%d" pid in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         let send ?key value =
           match Eio.Promise.await (Kafka_producer.produce_await producer ~topic ~value ?key ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
           | Ok () -> ()
         in
         send ~key:Bytes.empty (Some (Bytes.of_string "zero-length-key"));
         send (Some (Bytes.of_string "no-key"));
         send ~key:(Bytes.of_string "k") (Some (Bytes.of_string "real-key"));
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-keys-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let keys = ref [] in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           while List.length !keys < 3 do
             match Kafka_consumer.fetch consumer with
             | Ok msg -> keys := msg.key :: !keys
             | Error e -> Alcotest.failf "fetch failed: %s" (Kafka_error.to_string e)
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
        Kafka_consumer.close consumer

(* Regression note: a partition fiber that stopped processing
   without draining its own queue — on Stop, on exhausted retries, or on
   being interrupted mid-retry-sleep by another partition's stop — left
   that queue permanently full with nobody left to ever take from it
   again. routing_loop's or the shutdown None-sentinel's Eio.Stream.add
   into that abandoned queue then blocked forever, hanging the whole
   consume_partitioned call past its own documented "blocks until
   stopped, then returns" contract.

   Reproduction: whichever partition the handler sees first ("stuck")
   always errors, with retries bounded (max_attempts) so it is guaranteed
   to self-exhaust and call signal_stop within a few seconds even if
   routing_loop never manages to route anything from the other partition
   first — librdkafka's own per-partition fetch batching means routing
   order across partitions isn't controllable from here, and an earlier
   version of this test with unbounded retries could wedge routing_loop
   on the stuck partition's backlog before the other partition's message
   ever got a chance to trigger Stop at all, which is a real, documented,
   *pre-existing* routing_loop limitation (see the comment above
   routing_loop) — separate from, and not a regression test for, the bug
   this test is actually after. The first message from the other
   partition (if routing gets to it before the stuck partition
   self-exhausts) acks and returns Stop instead, exercising the specific
   cross-partition interrupt-mid-retry-sleep path. Either way, the whole
   call must still fully drain and return — before the fix, any of the
   three early-exit paths involved (own exhaustion, own Stop, or being
   interrupted by another partition's Stop) could abandon a full queue
   and hang forever; Eio.Time.with_timeout is a safety net so a
   regression fails this test cleanly instead of hanging the whole
   suite. *)
let test_consume_partitioned_stop_does_not_hang () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-partitioned-stop-%d" pid in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:2 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         for i = 0 to 9 do
           match Eio.Promise.await (Kafka_producer.produce_await producer
                   ~topic ~key:(Bytes.of_string (string_of_int i))
                   ~value:(Some (Bytes.of_string (string_of_int i))) ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
           | Ok () -> ()
         done;
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-partitioned-stop-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let stuck_partition = ref None in
        let handler (msg : Kafka_consumer.message) ~ack =
          match !stuck_partition with
          | None ->
            stuck_partition := Some msg.partition;
            Kafka_consumer.Error "stuck"
          | Some p when Int32.equal p msg.partition ->
            Kafka_consumer.Error "stuck"
          | Some _ ->
            ignore (ack ());
            Kafka_consumer.Stop
        in
        let retry : Kafka_consumer.retry_policy =
          { base_delay_s = 1.0; max_delay_s = 1.0; max_attempts = 3 }
        in
        let result =
          Eio.Time.with_timeout env#clock 20.0 (fun () ->
            Ok (Kafka_consumer.consume_partitioned consumer ~sw ~clock:env#clock
                  ~retry ~queue_capacity:3 ~handler ()))
        in
        (* Either termination is a legitimate, non-hanging outcome here:
           Ok () if the other partition's Stop won the race, Error "stuck"
           if the stuck partition self-exhausted first. What matters is
           that the call returns at all instead of hanging. *)
        (match result with
         | Error `Timeout ->
           Alcotest.fail "consume_partitioned hung — a stopped partition fiber \
                          left its queue abandoned-but-full"
         | Ok (Ok ()) -> ()
         | Ok (Error "stuck") -> ()
         | Ok (Error e) -> Alcotest.failf "unexpected handler error surfaced: %s" e);
        Kafka_consumer.close consumer

(* Regression note: librdkafka's default enable.auto.offset.store
   advances the "stored" position on every message poll_fiber prefetches
   into t.stream, whether or not the application ever processed it (or
   even took it off the stream) — so commit_all, which commits that
   stored position for a NULL partition list, could silently commit past
   messages the application never saw. Seed 5 messages, process (fetch +
   explicit commit) only the first, call commit_all, close, then confirm
   a fresh consumer in the same group still receives message index 1 —
   proving commit_all only committed what was explicitly committed
   elsewhere, not the whole prefetched backlog. *)
let test_commit_all_does_not_commit_past_processed () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-commit-all-%d" pid in
      let group_id = Printf.sprintf "kafka-eio-test-commit-all-group-%d" pid in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         for i = 0 to 4 do
           match Eio.Promise.await (Kafka_producer.produce_await producer
                   ~topic ~value:(Some (Bytes.of_string (string_of_int i))) ()) with
           | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
           | Ok () -> ()
         done;
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      (match Kafka_consumer.create cfg ~sw with
       | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
       | Ok consumer ->
         (* Give poll_fiber a moment to prefetch the whole backlog into
            t.stream before we process just one message — this is the
            precondition that made the stored position drift ahead of
            what was actually processed. *)
         Eio.Time.sleep env#clock 1.0;
         (match Kafka_consumer.fetch consumer with
          | Error e -> Alcotest.failf "fetch failed: %s" (Kafka_error.to_string e)
          | Ok msg ->
            (match Kafka_consumer.commit consumer msg with
             | Error e -> Alcotest.failf "commit failed: %s" (Kafka_error.to_string e)
             | Ok () -> ()));
         (match Kafka_consumer.commit_all consumer with
          | Error e -> Alcotest.failf "commit_all failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         Kafka_consumer.close consumer);

      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "second consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer2 ->
        let next = ref None in
        (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
           match Kafka_consumer.fetch consumer2 with
           | Ok msg -> next := Some msg; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka_error.to_string e))
         ) with
         | Error `Timeout ->
           Alcotest.fail "no message left after commit_all — it committed past \
                          the one message actually processed"
         | Error (`Fetch_failed m) -> Alcotest.failf "fetch failed: %s" m
         | Ok () -> ());
        let next = Option.get !next in
        Alcotest.(check (option string)) "commit_all did not commit past what was processed"
          (Some "1") (Option.map Bytes.to_string next.value);
        Kafka_consumer.close consumer2

(* Regression note: calling Kafka_consumer.close directly
   (instead of cancelling ~sw, the documented way to stop
   consume_partitioned) never resolved consume_partitioned's internal
   stop_p, so routing_loop's Fiber.first (racing Stream.take against
   stop_p) blocked forever once poll_fiber stopped feeding t.stream —
   hanging the call permanently. The handler here always returns
   Continue, so the only way this call can ever return is via the
   close-triggered watchdog. *)
let test_consume_partitioned_stops_on_direct_close () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-partitioned-close-%d" pid in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:1 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         (match Eio.Promise.await (Kafka_producer.produce_await producer
                  ~topic ~value:(Some (Bytes.of_string "x")) ()) with
          | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id     = Printf.sprintf "kafka-eio-test-partitioned-close-group-%d" pid;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "consumer create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        let handler _msg ~ack = ignore (ack ()); Kafka_consumer.Continue in
        let result =
          Eio.Time.with_timeout env#clock 10.0 (fun () ->
            Ok (Eio.Fiber.first
              (fun () ->
                 Kafka_consumer.consume_partitioned consumer ~sw ~clock:env#clock ~handler ())
              (fun () ->
                 Eio.Time.sleep env#clock 1.0;
                 Kafka_consumer.close consumer;
                 Eio.Time.sleep env#clock 30.0;
                 Ok ())))
        in
        (match result with
         | Error `Timeout ->
           Alcotest.fail "consume_partitioned hung after close was called directly"
         | Ok (Ok ()) -> ()
         | Ok (Error _) -> Alcotest.fail "unexpected handler error")

(* Regression note: last_processed (commit_all's source of
   truth when auto_commit = false) was never invalidated across a
   rebalance — a partition revoked from this consumer and later
   reassigned back could still have its stale, pre-revoke offset
   committed by commit_all, silently rolling the group's committed
   position backward past whatever a second consumer in the same group
   processed on that partition in the meantime.

   Reproduction: c1 alone owns both partitions of a fresh topic and acks
   everything on both. c2 then joins the same group, which (eager
   assignment) revokes c1's whole assignment and splits the 2 partitions
   between them; fresh messages are seeded so whichever partition(s) c2
   ends up with have new data; both consumers drain and ack whatever
   they can see. c2 then leaves, handing its partition(s) back to c1. c1
   — which fetched nothing new on the reclaimed partition — calls
   commit_all. A fresh consumer in the same group afterward must not
   receive anything c2 already committed; if it does, c1's commit_all
   rolled the group's committed offset backward. *)
let test_commit_all_survives_rebalance () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      let pid = Unix.getpid () in
      let topic = Printf.sprintf "kafka-eio-test-rebalance-%d" pid in
      let group_id = Printf.sprintf "kafka-eio-test-rebalance-group-%d" pid in
      let produce_range producer lo hi =
        for i = lo to hi do
          match Eio.Promise.await (Kafka_producer.produce_await producer
                  ~topic ~key:(Bytes.of_string (string_of_int i))
                  ~value:(Some (Bytes.of_string (Printf.sprintf "seed-%d" i))) ()) with
          | Error e -> Alcotest.failf "seed produce_await failed: %s" (Kafka_error.to_string e)
          | Ok () -> ()
        done
      in
      (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
       | Error e -> Alcotest.failf "seed producer create failed: %s" (Kafka_error.to_string e)
       | Ok producer ->
         (match Kafka_producer.create_topic producer
                  ~topic_name:topic ~partitions:2 ~replication_factor:1 with
          | Error e -> Alcotest.failf "create_topic failed: %s" (Kafka_error.to_string e)
          | Ok () -> ());
         produce_range producer 0 9;
         Kafka_producer.close producer);

      let cfg : Kafka_consumer.config = {
        brokers      = Kafka_test_helpers.brokers ();
        group_id;
        topics       = [ topic ];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      let drain_and_ack c ~budget_s =
        let deadline = Unix.gettimeofday () +. budget_s in
        let quiet_deadline = ref None in
        let rec loop () =
          if Unix.gettimeofday () > deadline then ()
          else
            match Kafka_consumer.poll c with
            | Error e -> Alcotest.failf "poll failed: %s" (Kafka_error.to_string e)
            | Ok None ->
              let now = Unix.gettimeofday () in
              (match !quiet_deadline with
               | Some t when now >= t -> ()
               | None -> Eio.Time.sleep env#clock 0.1; loop ()
               | Some _ -> Eio.Time.sleep env#clock 0.1; loop ())
            | Ok (Some msg) ->
              quiet_deadline := Some (Unix.gettimeofday () +. 0.5);
              (match Kafka_consumer.commit c msg with
               | Error e -> Alcotest.failf "commit failed: %s" (Kafka_error.to_string e)
               | Ok () -> ());
              loop ()
        in
        loop ()
      in
      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "c1 create failed: %s" (Kafka_error.to_string e)
      | Ok c1 ->
        drain_and_ack c1 ~budget_s:5.0;

        (* c2 joins the same group — triggers a rebalance splitting the 2
           partitions between c1 and c2. *)
        Eio.Switch.run (fun sw2 ->
          let c2_ready, c2_ready_r = Eio.Promise.create () in
          match Kafka_consumer.create cfg ~sw:sw2 ~on_ready:(fun () ->
                  Eio.Promise.resolve c2_ready_r ()) with
          | Error e -> Alcotest.failf "c2 create failed: %s" (Kafka_error.to_string e)
          | Ok c2 ->
            (match Eio.Time.with_timeout env#clock 10.0 (fun () ->
               Eio.Promise.await c2_ready; Ok ()) with
             | Ok () -> ()
             | Error `Timeout -> Alcotest.fail "timed out waiting for c2 assignment");
            (match Kafka_producer.create (Kafka_test_helpers.default_producer_config ()) ~sw with
             | Error e -> Alcotest.failf "second seed producer create failed: %s" (Kafka_error.to_string e)
             | Ok producer2 -> produce_range producer2 10 29; Kafka_producer.close producer2);
            drain_and_ack c1 ~budget_s:5.0;
            drain_and_ack c2 ~budget_s:5.0
          (* c2's switch ends here — closing it triggers another
             rebalance, handing its partition(s) back to c1. *));

        Eio.Time.sleep env#clock 3.0; (* let that rebalance settle too *)
        (match Kafka_consumer.commit_all c1 with
         | Error e -> Alcotest.failf "c1 commit_all failed: %s" (Kafka_error.to_string e)
         | Ok () -> ());
        Kafka_consumer.close c1;

      match Kafka_consumer.create cfg ~sw with
      | Error e -> Alcotest.failf "c3 create failed: %s" (Kafka_error.to_string e)
      | Ok c3 ->
        let unexpected = ref None in
        (match Eio.Time.with_timeout env#clock 2.0 (fun () ->
           match Kafka_consumer.fetch c3 with
           | Ok msg -> unexpected := Some msg; Ok ()
           | Error e -> Error (`Fetch_failed (Kafka_error.to_string e))
         ) with
         | Error `Timeout -> () (* expected: nothing left to redeliver *)
         | Error (`Fetch_failed m) -> Alcotest.failf "fetch failed: %s" m
         | Ok () ->
           let msg = Option.get !unexpected in
           Alcotest.failf
             "commit_all rolled back the group's committed offset — \
              re-received topic=%s partition=%ld offset=%Ld"
             msg.topic msg.partition msg.offset);
        Kafka_consumer.close c3

let () =
  let open Alcotest in
  run "kafka_consumer_integration" [
    "consume", [
      test_case "poll messages"    `Slow test_poll_messages;
      test_case "consume with ack" `Slow test_consume_with_ack;
      test_case "stream api"       `Slow test_stream_api;
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
