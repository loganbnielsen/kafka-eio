let brokers () =
  match Sys.getenv_opt "KAFKA_BROKERS" with
  | Some b when String.trim b <> "" -> [ b ]
  | _ -> [ "localhost:9092" ]

let broker_csv () = String.concat "," (brokers ())
