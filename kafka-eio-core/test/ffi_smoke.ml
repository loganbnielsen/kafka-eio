(** Smoke test for kafka_stubs.c — no Eio, no fibers, no threads.
    Verifies that every blocking stub correctly releases/reacquires the OCaml
    domain lock by calling each one sequentially and checking for crashes. *)

let () =
  Printf.printf "1: conf_new\n%!";
  let conf = Kafka_raw.conf_new () in

  Printf.printf "2: conf_set bootstrap.servers\n%!";
  (match Kafka_raw.conf_set conf "bootstrap.servers" (Kafka_test_brokers.broker_csv ()) with
   | Error s -> Printf.printf "FAIL conf_set brokers: %s\n%!" s; exit 1
   | Ok () -> ());

  Printf.printf "3: conf_set group.id\n%!";
  (match Kafka_raw.conf_set conf "group.id" "ffi-smoke-test" with
   | Error s -> Printf.printf "FAIL conf_set group.id: %s\n%!" s; exit 1
   | Ok () -> ());

  Printf.printf "4: kafka_new (consumer)\n%!";
  (match Kafka_raw.kafka_new Kafka_raw.Consumer conf (-1) with
   | Error s -> Printf.printf "FAIL kafka_new: %s\n%!" s; exit 1
   | Ok handle ->

     Printf.printf "5: assignment before subscribe\n%!";
     (match Kafka_raw.assignment handle with
      | [] -> Printf.printf "   -> []\n%!"
      | _ -> Printf.printf "FAIL fresh consumer unexpectedly has an assignment\n%!"; exit 1);

     Printf.printf "6: subscribe\n%!";
     (match Kafka_raw.subscribe handle ["ffi-smoke-topic"] with
      | Error s -> Printf.printf "FAIL subscribe: %s\n%!" s; exit 1
      | Ok () -> ());

     Printf.printf "7: consumer_poll (timeout=0, non-blocking)\n%!";
     (match Kafka_raw.consumer_poll handle 0 with
      | Timeout      -> Printf.printf "   -> Timeout (expected, no messages)\n%!"
      | Msg _        -> Printf.printf "   -> Msg (unexpected but not fatal)\n%!"
      | Poll_error e -> Printf.printf "   -> Poll_error %d (unexpected but not fatal)\n%!" e);

     Printf.printf "8: consumer_poll (timeout=100, blocking release/acquire)\n%!";
     (match Kafka_raw.consumer_poll handle 100 with
      | Timeout      -> Printf.printf "   -> Timeout\n%!"
      | Msg _        -> Printf.printf "   -> Msg\n%!"
      | Poll_error e -> Printf.printf "   -> Poll_error %d\n%!" e);

     Printf.printf "9: consumer_close\n%!";
     Kafka_raw.consumer_close handle;

     Printf.printf "10: destroy\n%!";
     Kafka_raw.destroy handle;

     Printf.printf "OK: all stubs passed\n%!")
