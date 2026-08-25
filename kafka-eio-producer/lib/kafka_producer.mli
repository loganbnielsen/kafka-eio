(** Eio-native Kafka producer built on kafka-eio-core. *)

type delivery_mode =
  | At_least_once
  | At_most_once
  | Exactly_once of { transaction_id : string }

type config = {
  brokers       : string list;
  delivery_mode : delivery_mode;
  linger_ms     : int option;          (** batch window; None = librdkafka default (5 ms) *)
  security      : Kafka_security.t;    (** transport security; use [Kafka_security.default] for plaintext dev *)
  properties    : (string * string) list;
  (** Raw librdkafka config keys applied after every field above, e.g.
      [("client.id", "checkout-svc"); ("statistics.interval.ms", "5000")].
      Use for librdkafka options this module has no typed field for.
      Overrides any of the typed fields' defaults for the same key. *)
}

(** A producer may be shared by any number of fibers in the Eio domain
    that created it (via [create]'s [~sw]). It is not domain-safe: sharing
    one [t] across multiple Eio domains is unsupported and unguarded. *)
type t

(** [create cfg ~sw] creates a producer and starts delivery and poll fibers
    in [sw]. When [sw] is cancelled the fibers stop and the producer is closed. *)
val create : config -> sw:Eio.Switch.t -> (t, Kafka_error.t) result

(** Every operation below returns [Error Kafka_error.Destroy] (or, for
    [produce_await], a promise already resolved to it) once [close] has been
    called, instead of touching the underlying (possibly destroyed) handle.
    [close] itself also resolves any [produce_await] promises still awaiting
    a delivery receipt to [Error Kafka_error.Destroy], so a fiber awaiting one
    cannot hang forever past shutdown. *)
val close : t -> unit

(** [create_topic t ~topic_name ~partitions ~replication_factor] creates a
    topic via librdkafka's admin API, reusing this producer's handle.
    Treats an already-existing topic as success. Does not leak
    [Kafka_raw.kafka_handle] the way an earlier [raw_handle] accessor did —
    [Kafka_raw] is internal to kafka-eio-core and not part of this
    package's public surface. *)
val create_topic
  :  t
  -> topic_name:string
  -> partitions:int
  -> replication_factor:int
  -> (unit, Kafka_error.t) result

(** Enqueue a message and return immediately. No delivery confirmation.
    [value = None] sends a Kafka tombstone (a NULL payload — the delete
    marker for a key on a compacted topic), distinct from
    [Some Bytes.empty], a genuine zero-length value. [value] is required
    (not optional) so a caller must choose explicitly, rather than a
    forgotten [~value] silently sending a tombstone. [~key], if given, is
    used exactly as passed — including a genuine zero-length key, which
    Kafka partitions differently (hashed) from no key at all (round-
    robin/sticky); omitting [~key] entirely means no key. A header's
    value is [string option]: [None] sends a NULL-valued header, distinct
    from [Some ""].

    Can return [Error Kafka_error.Queue_full] if librdkafka's local send
    queue is full — this call does not block or retry for you. Callers
    producing at a high rate must handle it themselves (drop, retry after
    a delay, or apply their own backpressure); there is currently no
    Eio-native blocking/backpressure variant of [produce].

    The trailing [unit] is required by OCaml's optional-argument erasure rules. *)
val produce
  :  t
  -> topic:string
  -> value:bytes option
  -> ?key:bytes
  -> ?headers:(string * string option) list
  -> unit
  -> (unit, Kafka_error.t) result

(** Enqueue a message and return a promise that resolves when the broker
    acknowledges delivery (or reports an error). [value = None] sends a
    tombstone, and [~key]/[~headers] behave exactly as in [produce]. Can
    resolve to [Error Kafka_error.Queue_full] the same way [produce] can
    return it — see [produce].
    The trailing [unit] is required by OCaml's optional-argument erasure rules. *)
val produce_await
  :  t
  -> topic:string
  -> value:bytes option
  -> ?key:bytes
  -> ?headers:(string * string option) list
  -> unit
  -> (unit, Kafka_error.t) result Eio.Promise.t

(** Block until all enqueued messages have been delivered. *)
val flush : t -> timeout_ms:int -> (unit, Kafka_error.t) result

(** A transactional-API call (begin/commit/abort/send-offsets) itself
    failed. [is_fatal]/[is_retriable]/[requires_abort] are librdkafka's
    per-error-instance flags — not derivable from [error] alone — telling
    the caller whether the producer must be retired, whether the same call
    can be retried, and whether the transaction had to be aborted (already
    done by [with_transaction] when this is [true]). *)
type txn_failure = {
  error          : Kafka_error.t;
  is_fatal       : bool;
  is_retriable   : bool;
  requires_abort : bool;
}

type transaction_error =
  | App_error of Kafka_error.t  (** [f] returned [Error _], or raised. *)
  | Txn_failure of txn_failure  (** a transactional-API call itself failed. *)

val string_of_transaction_error : transaction_error -> string

(** [with_transaction t ?consumer_offsets f] runs [f] inside a Kafka
    transaction. Commits on [Ok], aborts on [Error] or exception. Requires
    [delivery_mode = Exactly_once].

    [consumer_offsets], if given, is [(consumer_handle, offsets)] where
    [offsets] are the exact (topic, partition, offset-of-last-processed-
    message) tuples [f] processed from that consumer — typically built
    from the consumed messages' [topic]/[partition]/[offset] fields. Only
    these offsets are committed as part of the transaction; the consumer's
    current assignment or position is never read, so a transaction can
    never advance past a message [f] did not actually process. *)
val with_transaction
  :  t
  -> ?consumer_offsets:(Kafka_consumer_handle.t * (string * int32 * int64) list)
  -> (unit -> (unit, Kafka_error.t) result)
  -> (unit, transaction_error) result
