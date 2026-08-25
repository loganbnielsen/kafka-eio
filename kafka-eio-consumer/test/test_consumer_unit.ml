(** Unit tests for kafka-eio-consumer that don't require a live broker.
    Regression coverage: public operations
    must reject use after close instead of touching a destroyed handle. *)

let unreachable_config : Kafka_consumer.config =
  { brokers      = ["127.0.0.1:1"]
  ; group_id     = "test-unit-closed"
  ; topics       = ["test-unit-closed-topic"]
  ; offset_reset = Kafka_consumer.Latest
  ; auto_commit  = false
  ; security     = Kafka_security.default
  ; properties   = []
  }

let dummy_message : Kafka_consumer.message =
  { topic = "t"; partition = 0l; offset = 0L; key = None
  ; value = Some (Bytes.of_string "x"); timestamp = None; headers = [] }

let test_ops_after_close_return_destroy () =
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_consumer.create unreachable_config ~sw with
      | Error e -> Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok consumer ->
        Kafka_consumer.close consumer;
        let is_destroy = function Error Kafka_error.Destroy -> true | _ -> false in
        Alcotest.(check bool) "poll after close" true
          (is_destroy (Kafka_consumer.poll consumer));
        Alcotest.(check bool) "fetch after close" true
          (is_destroy (Kafka_consumer.fetch consumer));
        Alcotest.(check bool) "commit after close" true
          (is_destroy (Kafka_consumer.commit consumer dummy_message));
        Alcotest.(check bool) "commit_all after close" true
          (is_destroy (Kafka_consumer.commit_all consumer))

let () =
  let open Alcotest in
  run "kafka_consumer_unit" [
    "close_and_validation", [
      test_case "fetch/poll/commit/commit_all after close return Destroy" `Quick
        test_ops_after_close_return_destroy;
    ];
  ]
