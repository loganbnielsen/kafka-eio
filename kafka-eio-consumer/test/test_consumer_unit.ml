(** Unit tests for kafka-eio-consumer that don't require a live broker.
    Regression coverage: public operations
    must reject use after close instead of touching a destroyed handle. *)

let unreachable_config : Kafka.Consumer.config =
  { brokers      = ["127.0.0.1:1"]
  ; group_id     = "test-unit-closed"
  ; topics       = ["test-unit-closed-topic"]
  ; offset_reset = Kafka.Consumer.Latest
  ; auto_commit  = false
  ; security     = Kafka.Security.default
  ; properties   = []
  }

let dummy_message : Kafka.Consumer.message =
  { topic = "t"; partition = 0l; offset = 0L; key = None
  ; value = Some (Bytes.of_string "x"); timestamp = None; headers = [] }

let test_ops_after_close_return_destroy () =
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      match Kafka.Consumer.create unreachable_config ~sw with
      | Error e -> Alcotest.failf "create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        Kafka.Consumer.close consumer;
        let is_destroy = function Error Kafka.Error.Destroy -> true | _ -> false in
        Alcotest.(check bool) "poll after close" true
          (is_destroy (Kafka.Consumer.poll consumer));
        Alcotest.(check bool) "fetch after close" true
          (is_destroy (Kafka.Consumer.fetch consumer));
        Alcotest.(check bool) "commit after close" true
          (is_destroy (Kafka.Consumer.commit consumer dummy_message));
        Alcotest.(check bool) "commit_all after close" true
          (is_destroy (Kafka.Consumer.commit_all consumer));
        let handler_called = ref false in
        let consume_result =
          Kafka.Consumer.consume consumer
            ~handler:(fun _msg ~ack:_ ->
              handler_called := true;
              Kafka.Consumer.Stop)
            ()
        in
        Alcotest.(check bool) "consume after close" true
          (match consume_result with Ok () -> true | Error _ -> false);
        Alcotest.(check bool) "handler not called after close" false !handler_called

let test_blocked_consume_returns_destroy_on_close () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      match Kafka.Consumer.create unreachable_config ~sw with
      | Error e -> Alcotest.failf "create failed: %s" (Kafka.Error.to_string e)
      | Ok consumer ->
        let done_p, done_r = Eio.Promise.create () in
        Eio.Fiber.fork ~sw (fun () ->
          let result =
            Kafka.Consumer.consume consumer
              ~handler:(fun _msg ~ack:_ -> Kafka.Consumer.Continue)
              ()
          in
          Eio.Promise.resolve done_r result);
        Eio.Fiber.yield ();
        Kafka.Consumer.close consumer;
        let result =
          Eio.Time.with_timeout_exn env#clock 1.0 (fun () -> Eio.Promise.await done_p)
        in
        Alcotest.(check bool) "blocked consume returns Ok" true
          (match result with Ok () -> true | Error _ -> false)

let () =
  let open Alcotest in
  run "kafka_consumer_unit" [
    "close_and_validation", [
      test_case "operations after close return Destroy" `Quick
        test_ops_after_close_return_destroy;
      test_case "blocked consume returns Ok on close" `Quick
        test_blocked_consume_returns_destroy_on_close;
    ];
  ]
