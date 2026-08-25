(** Security transport configuration shared by producer and consumer. *)

type protocol =
  [ `Plaintext       (** No encryption or authentication. Local dev default. *)
  | `Ssl             (** TLS encryption, no SASL authentication. *)
  | `Sasl_plaintext  (** SASL authentication, no TLS encryption. *)
  | `Sasl_ssl        (** SASL authentication over TLS. Production default. *)
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

type t =
  | Plaintext
  | Ssl of { ssl_ca_location : string option }
  | Sasl_plaintext of sasl
  | Sasl_ssl of { ssl_ca_location : string option; sasl : sasl }

val default : t
(** [Plaintext]. Use for local dev and unit tests.
    Do not use in production — requires explicit [Ssl] or [Sasl_ssl]. *)

val protocol_of_string : string -> (protocol, string) result
(** Parse a finite Kafka security protocol value. Accepted values are
    ["plaintext"], ["ssl"], ["sasl_plaintext"], and ["sasl_ssl"], case
    insensitively. *)

val of_env : unit -> (t, string) result
(** Build from environment variables:
    - [KAFKA_SECURITY_PROTOCOL] — ["plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl"]
      (default: ["plaintext"], unknown values return [Error])
    - [KAFKA_SSL_CA_LOCATION]   — path to CA cert bundle
    - [KAFKA_SASL_MECHANISM]    — ["PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"]
    - [KAFKA_SASL_USERNAME]
    - [KAFKA_SASL_PASSWORD] *)

val apply : Kafka_raw.kafka_conf -> t -> (unit, string) result
(** Set librdkafka [security.protocol], [ssl.ca.location], and [sasl.*] keys on [conf].
    Returns [Error msg] if any key is rejected by librdkafka. SASL credentials
    are required by the [Sasl_plaintext] and [Sasl_ssl] constructors.
    Called internally by producer and consumer [conf_of_config]; not part of the public API. *)
