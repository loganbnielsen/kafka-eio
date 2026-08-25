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
    to configured topics, and starts a poll fiber in [sw]. [on_ready] is
    called exactly once when the broker assigns partitions to this consumer —
    use it to signal readiness instead of sleeping for a fixed rebalance
    timeout. [on_poll_error] receives a raw librdkafka error code for any
    message-level poll error (auth failure, unknown topic, max-poll-interval
    exceeded, ...) other than end-of-partition, which is not an error;
    defaults to logging to stderr. Without this, such errors were previously
    indistinguishable from "no message available", so a consumer could spin
    forever never reporting that it was dead or unauthorized. *)
val create
  :  ?on_ready:(unit -> unit)
  -> ?on_poll_error:(int -> unit)
  -> config
  -> sw:Eio.Switch.t
  -> (t, Kafka_error.t) result

(** Every operation below returns [Error Kafka_error.Destroy] once [close]
    has been called, instead of touching the underlying (possibly destroyed)
    handle. *)
val close : t -> unit

(** Expose the underlying handle for use with transactional producers. *)
val handle : t -> Kafka_consumer_handle.t

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
    Returns [Ok ()] when [handler] returns [Stop], [Error e] when [handler]
    returns [Error e]. *)
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
    partition's offset as of the last explicit [commit]/[ack] call, not
    the last message merely fetched into [stream]/[fetch]/[poll]. With
    [auto_commit = true], this instead commits each partition's last
    *fetched* offset (librdkafka's auto-stored position, the same one
    periodic auto-commit uses) — combining [commit_all] with
    [auto_commit = true] therefore carries the same risk [auto_commit]
    itself already documents: an offset can advance before the message
    is actually processed. *)
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

(** [consume_partitioned t ~sw ~clock ?retry ?on_retry ?on_warning
    ?queue_capacity ~handler] is like [consume] but routes each message to a
    dedicated per-partition fiber. A partition's retry sleep blocks only that
    partition; other partitions continue unaffected. During retry sleep the
    partition is paused at the librdkafka level so no messages accumulate in
    its stream buffer.

    Processing within a single partition is strictly sequential — [ack]
    commits by offset, so out-of-order acknowledgement within a partition
    would silently advance past unprocessed messages. Do not process two
    messages from the same partition concurrently against one [t].

    [queue_capacity] bounds each partition's message queue (default
    [default_queue_capacity]). Routing is a single loop that dispatches
    each message with a plain, synchronous, blocking add into its
    partition's queue — deliberately, not from a per-message fiber — so a
    partition whose queue is at capacity *does* stall routing to every
    other partition until it drains. This is a bounded stall (by
    [queue_capacity], and by [pause_partition] already halting new
    deliveries to a partition during its own retry sleep, the most common
    cause of backlog), accepted in exchange for a hard memory bound —
    forking a fiber per message to avoid it was tried and reverted, since
    it replaces a bounded stall with unbounded fiber/memory growth.

    [on_retry ~partition ~attempt ~delay_s] is called just before each
    retry sleep — use it to increment metrics counters. [on_warning] receives
    human-readable text for ack-misuse and retry/exhaustion events; defaults
    to stderr.

    The function blocks until the consumer is stopped (handler returns [Stop]
    or retries are exhausted), then returns. All partition fibers are joined
    before returning, so the consumer handle is safe to close immediately after.

    [~sw] is accepted for signature consistency with every other entry
    point in this library, but — unlike those — nothing is explicitly
    forked under it: internally, this function runs its own
    [Eio.Switch.run] so every partition fiber is provably joined before
    returning even if [~sw] is cancelled mid-call, which a fiber forked
    directly under [~sw] could not guarantee. This does not weaken
    cancellation: [consume_partitioned] runs synchronously within the
    calling fiber, so cancelling [~sw] (or any ancestor switch) still
    stops this call exactly as it would any other blocking call made
    from a fiber registered under it. Calling [close] directly on [t]
    from another fiber also stops it, polled at a 100ms granularity —
    prefer cancelling [~sw] for a tighter bound. *)
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
  -> (unit, 'e) result
