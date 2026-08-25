type protocol =
  [ `Plaintext
  | `Ssl
  | `Sasl_plaintext
  | `Sasl_ssl
  ]

type sasl_mechanism =
  | Plain
  | Scram_sha256
  | Scram_sha512

type sasl = {
  mechanism : sasl_mechanism;
  username  : string;
  password  : string;
}

let mechanism_to_string = function
  | Plain         -> "PLAIN"
  | Scram_sha256  -> "SCRAM-SHA-256"
  | Scram_sha512  -> "SCRAM-SHA-512"

let mechanism_of_string value =
  match String.uppercase_ascii value with
  | "PLAIN"          -> Ok Plain
  | "SCRAM-SHA-256"  -> Ok Scram_sha256
  | "SCRAM-SHA-512"  -> Ok Scram_sha512
  | other ->
    Error
      (Printf.sprintf
         "kafka security: unknown KAFKA_SASL_MECHANISM %S \
          (expected PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)"
         other)

type t =
  | Plaintext
  | Ssl of { ssl_ca_location : string option }
  | Sasl_plaintext of sasl
  | Sasl_ssl of { ssl_ca_location : string option; sasl : sasl }

let default = Plaintext

let protocol_to_string = function
  | `Plaintext      -> "plaintext"
  | `Ssl            -> "ssl"
  | `Sasl_plaintext -> "sasl_plaintext"
  | `Sasl_ssl       -> "sasl_ssl"

let protocol_of_string value =
  match String.lowercase_ascii value with
  | "plaintext"      -> Ok `Plaintext
  | "ssl"            -> Ok `Ssl
  | "sasl_plaintext" -> Ok `Sasl_plaintext
  | "sasl_ssl"       -> Ok `Sasl_ssl
  | other ->
    Error
      (Printf.sprintf
         "kafka security: unknown KAFKA_SECURITY_PROTOCOL %S \
          (expected plaintext, ssl, sasl_plaintext, or sasl_ssl)"
         other)

let env_opt name =
  match Sys.getenv_opt name with Some v when v <> "" -> Some v | _ -> None

let required_env name =
  match env_opt name with
  | Some value -> Ok value
  | None       -> Error ("kafka security: " ^ name ^ " required for SASL protocols")

let sasl_of_env () =
  let ( let* ) = Result.bind in
  let* mechanism_raw = required_env "KAFKA_SASL_MECHANISM" in
  let* mechanism     = mechanism_of_string mechanism_raw in
  let* username      = required_env "KAFKA_SASL_USERNAME" in
  let* password      = required_env "KAFKA_SASL_PASSWORD" in
  Ok { mechanism; username; password }

let of_env () =
  let ( let* ) = Result.bind in
  let* protocol =
    match env_opt "KAFKA_SECURITY_PROTOCOL" with
    | None       -> Ok `Plaintext
    | Some value -> protocol_of_string value
  in
  let ssl_ca_location = env_opt "KAFKA_SSL_CA_LOCATION" in
  match protocol with
  | `Plaintext      -> Ok Plaintext
  | `Ssl            -> Ok (Ssl { ssl_ca_location })
  | `Sasl_plaintext ->
    let* sasl = sasl_of_env () in
    Ok (Sasl_plaintext sasl)
  | `Sasl_ssl ->
    let* sasl = sasl_of_env () in
    Ok (Sasl_ssl { ssl_ca_location; sasl })

let apply conf t =
  let errs = ref [] in
  let set k v = match Kafka_raw.conf_set conf k v with
    | Ok ()   -> ()
    | Error s -> errs := ("kafka security conf " ^ k ^ ": " ^ s) :: !errs
  in
  let set_sasl sasl =
    set "sasl.mechanism" (mechanism_to_string sasl.mechanism);
    set "sasl.username" sasl.username;
    set "sasl.password" sasl.password
  in
  (match t with
   | Plaintext ->
     set "security.protocol" (protocol_to_string `Plaintext)
   | Ssl { ssl_ca_location } ->
     set "security.protocol" (protocol_to_string `Ssl);
     Option.iter (set "ssl.ca.location") ssl_ca_location
   | Sasl_plaintext sasl ->
     set "security.protocol" (protocol_to_string `Sasl_plaintext);
     set_sasl sasl
   | Sasl_ssl { ssl_ca_location; sasl } ->
     set "security.protocol" (protocol_to_string `Sasl_ssl);
     Option.iter (set "ssl.ca.location") ssl_ca_location;
     set_sasl sasl);
  match !errs with
  | []   -> Ok ()
  | errs -> Error (String.concat "; " (List.rev errs))
