let test_accepts_valid_names () =
  List.iter
    (fun name ->
      match Kafka.Topic_name.of_string name with
      | Ok t -> Alcotest.(check string) name name (Kafka.Topic_name.to_string t)
      | Error e -> Alcotest.failf "%s should be valid: %s" name e)
    [ "orders"; "sun-demo-orders"; "payments.charges_v1"; "__consumer_offsets";
      String.make 249 'a' ]

let test_rejects_invalid_names () =
  let long_name = String.make 250 'a' in
  List.iter
    (fun name ->
      match Kafka.Topic_name.of_string name with
      | Ok _ -> Alcotest.failf "%S should be invalid" name
      | Error _ -> ())
    [ ""; "."; ".."; "orders/v1"; "orders v1"; long_name ]

let test_of_string_exn_raises_on_invalid () =
  match Kafka.Topic_name.of_string_exn "" with
  | _ -> Alcotest.fail "expected Invalid_argument for an empty topic name"
  | exception Invalid_argument _ -> ()

let test_of_string_exn_returns_on_valid () =
  let t = Kafka.Topic_name.of_string_exn "orders" in
  Alcotest.(check string) "round-trips" "orders" (Kafka.Topic_name.to_string t)

let () =
  let open Alcotest in
  run "kafka_topic_name" [
    "of_string", [
      test_case "accepts Kafka-compatible names" `Quick test_accepts_valid_names;
      test_case "rejects invalid names"          `Quick test_rejects_invalid_names;
    ];
    "of_string_exn", [
      test_case "raises on invalid"  `Quick test_of_string_exn_raises_on_invalid;
      test_case "returns on valid"   `Quick test_of_string_exn_returns_on_valid;
    ];
  ]
