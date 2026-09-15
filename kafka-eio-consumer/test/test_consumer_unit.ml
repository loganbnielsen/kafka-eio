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
  Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
      match Kafka.Consumer.create ~clock:env#clock unreachable_config ~sw with
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
      match Kafka.Consumer.create ~clock:env#clock unreachable_config ~sw with
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

(* FEAT-078: retry_policy's shared backoff schedule -- bounded, non-negative,
   and deterministic given an injected rng (never the bare global Random
   module). *)
let policy : Kafka.Consumer.retry_policy =
  { base_delay_s = 1.0; max_delay_s = 10.0; max_attempts = -1; jitter_ratio = 0.2 }

let test_backoff_s_early_attempt_within_jittered_bounds () =
  let rng = Random.State.make [| 42 |] in
  let raw = policy.base_delay_s *. (2. ** Float.of_int (2 - 1)) in
  let delay = Kafka.Consumer.backoff_s ~rng policy 2 in
  Alcotest.(check bool)
    "within +-20% of the raw exponential delay"
    true
    (delay >= raw *. 0.8 && delay <= raw *. 1.2)

let test_backoff_s_caps_at_max_delay () =
  let rng = Random.State.make [| 7 |] in
  for attempt = 1 to 30 do
    let delay = Kafka.Consumer.backoff_s ~rng policy attempt in
    Alcotest.(check bool)
      (Printf.sprintf "attempt %d never exceeds max_delay_s" attempt)
      true
      (delay <= policy.max_delay_s)
  done

let test_backoff_s_never_negative () =
  let rng = Random.State.make [| 99 |] in
  for attempt = 1 to 10 do
    let delay = Kafka.Consumer.backoff_s ~rng policy attempt in
    Alcotest.(check bool)
      (Printf.sprintf "attempt %d never negative" attempt)
      true
      (delay >= 0.0)
  done

let test_backoff_s_deterministic_with_same_seed () =
  let delay1 = Kafka.Consumer.backoff_s ~rng:(Random.State.make [| 5 |]) policy 3 in
  let delay2 = Kafka.Consumer.backoff_s ~rng:(Random.State.make [| 5 |]) policy 3 in
  Alcotest.(check (float 0.0)) "same seed, same delay" delay1 delay2

let test_backoff_s_no_jitter_when_ratio_zero () =
  let policy = { policy with jitter_ratio = 0.0 } in
  let rng = Random.State.make [| 1 |] in
  Alcotest.(check (float 0.0001))
    "exact exponential delay"
    2.0
    (Kafka.Consumer.backoff_s ~rng policy 2)

let () =
  let open Alcotest in
  run "kafka_consumer_unit" [
    "close_and_validation", [
      test_case "operations after close return Destroy" `Quick
        test_ops_after_close_return_destroy;
      test_case "blocked consume returns Ok on close" `Quick
        test_blocked_consume_returns_destroy_on_close;
    ];
    "backoff_s", [
      test_case "early attempt within jittered bounds" `Quick
        test_backoff_s_early_attempt_within_jittered_bounds;
      test_case "caps at max delay" `Quick
        test_backoff_s_caps_at_max_delay;
      test_case "never negative" `Quick
        test_backoff_s_never_negative;
      test_case "deterministic with the same seed" `Quick
        test_backoff_s_deterministic_with_same_seed;
      test_case "no jitter when jitter_ratio is 0" `Quick
        test_backoff_s_no_jitter_when_ratio_zero;
    ];
  ]
