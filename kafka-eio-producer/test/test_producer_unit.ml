(** Unit tests for kafka-eio-producer that don't require a live broker:
    use-after-close, and close resolving pending produce_await.

    topic_new's null-pointer failure path (Kafka_raw.topic_new returning a
    result) has no test here: this librdkafka build doesn't validate topic
    name syntax locally, so there's no reliable local trigger for it. *)

let unreachable_config : Kafka_producer.config =
  { brokers       = ["127.0.0.1:1"]
  ; delivery_mode = Kafka_producer.At_least_once
  ; linger_ms     = None
  ; security      = Kafka_security.default
  ; properties    = []
  }

let test_produce_after_close_is_destroyed () =
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_producer.create unreachable_config ~sw with
      | Error e -> Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        Kafka_producer.close producer;
        (match Kafka_producer.produce producer ~topic:"t" ~value:(Some (Bytes.of_string "x")) () with
         | Error Kafka_error.Destroy -> ()
         | Ok () -> Alcotest.fail "expected produce after close to fail"
         | Error e -> Alcotest.failf "expected Destroy, got %s" (Kafka_error.to_string e));
        (match Kafka_producer.flush producer ~timeout_ms:0 with
         | Error Kafka_error.Destroy -> ()
         | Ok () -> Alcotest.fail "expected flush after close to fail"
         | Error e -> Alcotest.failf "expected Destroy, got %s" (Kafka_error.to_string e))

let test_close_resolves_pending_produce_await () =
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      match Kafka_producer.create unreachable_config ~sw with
      | Error e -> Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
      | Ok producer ->
        let promise =
          Kafka_producer.produce_await producer ~topic:"t" ~value:(Some (Bytes.of_string "x")) ()
        in
        (* close flushes (up to 5s) against an unreachable broker, then must
           still resolve this promise rather than leave it pending forever. *)
        Kafka_producer.close producer;
        (match Eio.Time.with_timeout env#clock 10.0
                 (fun () -> Ok (Eio.Promise.await promise)) with
         | Error `Timeout ->
           Alcotest.fail "produce_await promise never resolved after close"
         | Ok (Ok ()) ->
           Alcotest.fail "expected the pending promise to resolve to Error after close"
         | Ok (Error _) -> ())

(* Guards against close leaking the delivery/wake pipes — Eio_unix.pipe
   ties their fd lifetime to the switch, not the producer value, so close
   must close them explicitly. *)
let test_close_does_not_leak_pipe_fds () =
  if not (Sys.file_exists "/proc/self/fd") then
    Alcotest.skip ();
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      let fd_count () = Array.length (Sys.readdir "/proc/self/fd") in
      let before = fd_count () in
      for _ = 1 to 5 do
        match Kafka_producer.create unreachable_config ~sw with
        | Error e -> Alcotest.failf "create failed: %s" (Kafka_error.to_string e)
        | Ok producer -> Kafka_producer.close producer
      done;
      let after = fd_count () in
      Alcotest.(check bool)
        (Printf.sprintf "fd count stable across 5 create/close cycles (before=%d after=%d)"
           before after)
        true (after <= before + 2)

let () =
  let open Alcotest in
  run "kafka_producer_unit" [
    "close_and_validation", [
      test_case "produce/flush after close return Destroy" `Quick
        test_produce_after_close_is_destroyed;
      test_case "close resolves pending produce_await" `Quick
        test_close_resolves_pending_produce_await;
      test_case "close does not leak pipe fds" `Quick
        test_close_does_not_leak_pipe_fds;
    ];
  ]
