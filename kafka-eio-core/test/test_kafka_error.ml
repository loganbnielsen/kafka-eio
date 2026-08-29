let () =
  let open Alcotest in
  run "kafka_error" [
    "of_int", [
      test_case "no_error round-trips" `Quick (fun () ->
        check bool "no_error"
          true
          (Kafka.Error.of_int 0 = Kafka.Error.No_error));

      test_case "positive protocol errors" `Quick (fun () ->
        let open Kafka.Error in
        check bool "offset_out_of_range" true (of_int 1 = Offset_out_of_range);
        check bool "leader_not_available" true (of_int 5 = Leader_not_available);
        check bool "request_timed_out"    true (of_int 7 = Request_timed_out));

      test_case "negative internal errors" `Quick (fun () ->
        let open Kafka.Error in
        check bool "unknown"          true (of_int (-1)   = Unknown);
        check bool "timed_out"        true (of_int (-185) = Timed_out);
        check bool "queue_full"       true (of_int (-184) = Queue_full);
        check bool "all_brokers_down" true (of_int (-187) = All_brokers_down));

      test_case "unknown codes produce Err_unknown" `Quick (fun () ->
        let open Kafka.Error in
        check bool "unknown_999"  true (of_int 999   = Err_unknown 999);
        check bool "unknown_-999" true (of_int (-999) = Err_unknown (-999)));
    ];

    "is_retryable", [
      test_case "retryable errors" `Quick (fun () ->
        let open Kafka.Error in
        check bool "leader_not_available" true  (is_retryable Leader_not_available);
        check bool "timed_out"            true  (is_retryable Timed_out);
        check bool "all_brokers_down"     true  (is_retryable All_brokers_down));

      test_case "non-retryable errors" `Quick (fun () ->
        let open Kafka.Error in
        check bool "no_error"           false (is_retryable No_error);
        check bool "invalid_msg"        false (is_retryable Invalid_msg);
        check bool "msg_size_too_large" false (is_retryable Msg_size_too_large));
    ];

    "is_fatal", [
      test_case "fatal errors" `Quick (fun () ->
        let open Kafka.Error in
        check bool "fatal"   true (is_fatal Fatal);
        check bool "destroy" true (is_fatal Destroy));

      test_case "non-fatal errors" `Quick (fun () ->
        let open Kafka.Error in
        check bool "no_error"  false (is_fatal No_error);
        check bool "timed_out" false (is_fatal Timed_out));
    ];
  ]
