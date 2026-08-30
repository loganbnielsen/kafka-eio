(** Eio-native Kafka consumer built on kafka-eio-core. *)

(** Outcome of processing one message. Polymorphic in the application's own
    error type — this library never forces [Kafka_error.t] on handler
    failures, since most handler errors (decode, database, HTTP, ...) have
    nothing to do with Kafka. *)
type 'e handler_result =
  | Continue
  | Stop
  | Error of 'e

type offset_reset =
  | Earliest
  | Latest

type config = {
  brokers      : string list;
  group_id     : string;
  topics       : string list;
  offset_reset : offset_reset;
  auto_commit  : bool;
  security     : Kafka_security.t;    (** transport security; use [Kafka_security.default] for plaintext dev *)
  properties   : (string * string) list;
  (** Raw librdkafka config keys applied after every field above, e.g.
      [("client.id", "checkout-worker"); ("max.poll.interval.ms", "600000")].
      Use for librdkafka options this module has no typed field for.
      Overrides any of the typed fields' defaults for the same key. *)
}

type message = {
  topic     : string;
  partition : int32;
  offset    : int64;
  key       : bytes option;
  value     : bytes option;
  (** [None] is a Kafka tombstone (a delete marker on a compacted topic) —
      distinct from [Some Bytes.empty], a genuine zero-length value. *)
  timestamp : int64 option;
  headers   : (string * string option) list;
  (** e.g. [("traceparent", Some "00-...")]. A header's value is [None]
      when the header was set with a NULL value, distinct from [Some ""]. *)
}

(** A consumer may be shared by any number of fibers in the Eio domain
    that created it (via [create]'s [~sw]). It is not domain-safe: sharing
    one [t] across multiple Eio domains is unsupported and unguarded.

    [stream], [fetch], and [poll] all read from the same internal queue,
    so mixing calls to them is safe but nondeterministic about which call
    gets which message. [consume] and [consume_partitioned] are full
    driving loops meant to own that queue exclusively — do not also call
    [fetch]/[poll]/[stream] while one of them is running. *)
type t

(** [create ?on_ready ?on_poll_error cfg ~sw] creates a consumer, subscribes
    to configured topics, and starts a poll fiber in [sw]. [on_ready] fires
    once when the broker assigns partitions — use it instead of sleeping for
    a fixed rebalance timeout. [on_poll_error] receives a raw librdkafka
    error code for any message-level poll error other than end-of-partition
    (which is not an error); without it such errors are indistinguishable
    from "no message available", letting a dead/unauthorized consumer spin
    forever unnoticed. Defaults to logging to stderr. *)
val create
  :  ?on_ready:(unit -> unit)
  -> ?on_poll_error:(int -> unit)
  -> config
  -> sw:Eio.Switch.t
  -> (t, Kafka_error.t) result

(** [close t] releases the consumer. Single-message operations below return
    [Error Kafka_error.Destroy] once [close] has been called. Driving loops
    such as [consume] and [consume_partitioned] stop normally. *)
val close : t -> unit

(** Opaque handle for {!Kafka_producer.with_transaction}'s
    [consumer_offsets] argument. *)
val handle : t -> Kafka_producer.consumer_handle

(** Returns an Eio stream that yields messages as they arrive.
    Backpressure is applied via stream capacity. *)
val stream : t -> message Eio.Stream.t

(** [fetch t] blocks until the next message is available and returns it.
    The direct-style equivalent of [Eio.Stream.take (stream t)] — prefer
    this for a plain consume loop; reach for [stream] directly only when
    you need Eio's other [Stream] operations (e.g. [take_nonblocking]).
    Returns [Error Kafka_error.Destroy] immediately if [t] is already
    closed; a close that happens while a [fetch] call is already blocked
    does not cancel it, the same as blocking on [stream] directly. *)
val fetch : t -> (message, Kafka_error.t) result

(** Process messages in a loop. [ack ()] commits the offset for the current
    message and returns the result of that commit — a synchronous librdkafka
    call that can itself fail. [on_warning] receives a human-readable message
    for API-misuse/operational events (e.g. a handler returning without
    calling [ack ()]); defaults to writing to stderr prefixed [kafka-eio: ].
    Returns [Ok ()] when [handler] returns [Stop] or [t] is closed,
    [Error e] when [handler] returns [Error e]. *)
val consume
  :  t
  -> ?on_warning:(string -> unit)
  -> handler:(message -> ack:(unit -> (unit, Kafka_error.t) result) -> 'e handler_result)
  -> unit
  -> (unit, 'e) result

(** Non-blocking check for one message. Returns [None] immediately if the
    queue is empty. Use [stream] for blocking, backpressure-aware delivery. *)
val poll : t -> (message option, Kafka_error.t) result

(** Commit offset for a specific message (synchronous). *)
val commit : t -> message -> (unit, Kafka_error.t) result

(** Commit all currently assigned partitions' offsets (synchronous) — each
    partition's offset as of the last explicit [commit]/[ack], not the
    last message merely fetched. With [auto_commit = true], commits each
    partition's last auto-stored fetch position instead, carrying the
    same risk [auto_commit] documents: an offset can advance before the
    message is actually processed. *)
val commit_all : t -> (unit, Kafka_error.t) result

(** Retry policy for [consume_partitioned]. *)
type retry_policy = {
  base_delay_s : float;
  (** Initial backoff in seconds; doubles on each consecutive failure. *)
  max_delay_s  : float;
  (** Backoff cap. Default: [600.0] (10 minutes). *)
  max_attempts : int;
  (** Maximum attempts. Negative = retry indefinitely. Default: [-1]. *)
}

val default_retry : retry_policy

val default_queue_capacity : int
(** [16]. Default bound for each partition's in-memory message queue in
    [consume_partitioned]. *)

type 'e consume_error =
  | Handler_errors of (int32 * 'e) list
  | Invalid_config of string
(** Result error for [consume_partitioned]: either exhausted handler errors by
    partition, or invalid consumer-loop configuration rejected before polling. *)

(** [consume_partitioned t ~sw ~clock ?retry ?on_retry ?on_warning
    ?queue_capacity ~handler] is like [consume] but routes each message to a
    dedicated per-partition fiber, so retry backoff on one partition doesn't
    block others. During retry sleep the partition is paused at the
    librdkafka level so its stream doesn't accumulate messages.

    Processing within a partition is strictly sequential — do not process
    two messages from the same partition concurrently against one [t], since
    out-of-order [ack] would silently skip unprocessed messages.

    [queue_capacity] bounds each partition's queue (default
    [default_queue_capacity]); routing dispatches synchronously rather than
    via a per-message fiber, so a full partition queue stalls routing to
    every other partition until it drains — a deliberate tradeoff for a
    hard memory bound over unbounded fiber growth. Must be positive.

    [on_retry ~partition ~attempt ~delay_s] fires just before each retry
    sleep. [on_warning] receives text for ack-misuse and retry/exhaustion
    events; defaults to stderr.

    Blocks until the consumer stops (handler returns [Stop] or retries are
    exhausted); all partition fibers are joined before returning, so [t] is
    safe to close immediately after.

    [~sw] matches this library's other entry points' signature, but nothing
    is forked under it directly — internally this runs its own
    [Eio.Switch.run] so partition fibers are joined even if [~sw] is
    cancelled mid-call. This doesn't weaken cancellation: the function still
    runs synchronously within the calling fiber, so cancelling [~sw] stops
    it like any other blocking call. Calling [close] on [t] from another
    fiber also stops it, but only polled at 100ms granularity — prefer
    cancelling [~sw] for a tighter bound. *)
val consume_partitioned
  :  t
  -> sw:Eio.Switch.t
  -> clock:_ Eio.Time.clock
  -> ?retry:retry_policy
  -> ?on_retry:(partition:int32 -> attempt:int -> delay_s:float -> unit)
  -> ?on_warning:(string -> unit)
  -> ?queue_capacity:int
  -> handler:(message -> ack:(unit -> (unit, Kafka_error.t) result) -> 'e handler_result)
  -> unit
  -> (unit, 'e consume_error) result
