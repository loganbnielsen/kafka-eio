(** Sun framework demo: produce 5 messages, consume them back, print results. *)

let brokers =
  match Sys.getenv_opt "KAFKA_BROKERS" with
  | Some b -> [b]
  | None   -> ["localhost:9092"]

let topic = "sun-demo"

let () =
  Eio_main.run @@ fun _ ->
    Eio.Switch.run @@ fun sw ->

      (* ---- Producer ---- *)
      let producer_cfg : Kafka_producer.config = {
        brokers;
        delivery_mode = Kafka_producer.At_least_once;
        linger_ms = None;
        security  = Kafka_security.default;
        properties = [];
      } in
      let producer =
        match Kafka_producer.create producer_cfg ~sw with
        | Error e ->
          Printf.eprintf "Failed to create producer: %s\n%!" (Kafka_error.to_string e);
          exit 1
        | Ok p -> p
      in

      Printf.printf "Producing 5 messages to topic '%s'...\n%!" topic;
      let promises = List.init 5 (fun i ->
        let value = Some (Bytes.of_string (Printf.sprintf "sun-message-%d" i)) in
        let p = Kafka_producer.produce_await producer ~topic ~value () in
        Printf.printf "  enqueued: sun-message-%d\n%!" i;
        p
      ) in

      List.iteri (fun i p ->
        match Eio.Promise.await p with
        | Ok ()   -> Printf.printf "  delivered: sun-message-%d\n%!" i
        | Error e -> Printf.eprintf "  delivery error %d: %s\n%!" i (Kafka_error.to_string e)
      ) promises;

      Printf.printf "All messages delivered.\n%!";
      Kafka_producer.close producer;

      (* ---- Consumer ---- *)
      let consumer_cfg : Kafka_consumer.config = {
        brokers;
        group_id     = "sun-demo-group";
        topics       = [topic];
        offset_reset = Kafka_consumer.Earliest;
        auto_commit  = false;
        security     = Kafka_security.default;
        properties   = [];
      } in
      let consumer =
        match Kafka_consumer.create consumer_cfg ~sw with
        | Error e ->
          Printf.eprintf "Failed to create consumer: %s\n%!" (Kafka_error.to_string e);
          exit 1
        | Ok c -> c
      in

      Printf.printf "\nConsuming from topic '%s'...\n%!" topic;
      let count = ref 0 in
      let _ =
        Kafka_consumer.consume consumer ~handler:(fun msg ~ack ->
          Printf.printf "  received [partition=%ld offset=%Ld]: %s\n%!"
            msg.Kafka_consumer.partition
            msg.Kafka_consumer.offset
            (match msg.Kafka_consumer.value with
             | Some v -> Bytes.to_string v
             | None -> "<tombstone>");
          ignore (ack ());
          incr count;
          if !count >= 5 then Kafka_consumer.Stop
          else Kafka_consumer.Continue
        ) ()
      in
      Printf.printf "Done. Consumed %d messages.\n%!" !count;
      Kafka_consumer.close consumer
