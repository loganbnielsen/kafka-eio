(** Validated Kafka topic name.

    Kafka-compatible names are 1-249 bytes, may contain ASCII letters,
    digits, [.], [_], and [-], and may not be [.] or [..]. *)
type t = private string

val of_string : string -> (t, string) result
(** Validate and construct a Kafka topic name. *)

val of_string_exn : string -> t
(** Like {!of_string}, but raises [Invalid_argument] when the name is
    invalid. Intended for static topic-name literals (a source-code
    constant), not runtime data. *)

val to_string : t -> string
