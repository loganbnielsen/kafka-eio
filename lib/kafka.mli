(** Canonical public API for kafka-eio. *)

module Error = Kafka_error

module Security = Kafka_security

module Topic_name = Kafka_topic_name

module Consumer : sig
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
    security     : Security.t;
    properties   : (string * string) list;
  }

  type message = {
    topic     : string;
    partition : int32;
    offset    : int64;
    key       : bytes option;
    value     : bytes option;
    timestamp : int64 option;
    headers   : (string * string option) list;
  }

  type t

  type hooks =
    { on_ready : unit -> unit
    ; on_assigned : unit -> unit
    ; on_revoked : unit -> unit
    ; on_poll : unit -> unit
    ; on_poll_error : int -> unit
    ; on_warning : string -> unit
    ; on_retry : partition:int32 -> attempt:int -> delay_s:float -> unit
    }

  val default_hooks : hooks

  val create
    :  ?hooks:hooks
    -> clock:_ Eio.Time.clock
    -> config
    -> sw:Eio.Switch.t
    -> (t, Error.t) result
  (** Lifecycle/progress observations for callers that model readiness and
      liveness themselves. [on_assigned]/[on_revoked] fire on assignment
      transitions (a non-empty to different-non-empty rebalance fires
      [on_revoked] then [on_assigned]); [on_poll] fires after every successful
      poll, message or not; [on_ready] is the compatibility one-shot on the
      first assignment. This library carries no policy about what those
      observations mean. *)

  val close : t -> unit
  val fetch : t -> (message, Error.t) result
  val poll : t -> (message option, Error.t) result
  val commit : t -> message -> (unit, Error.t) result
  val commit_all : t -> (unit, Error.t) result

  val pause_partition : t -> topic:string -> partition:int32 -> (unit, Error.t) result
  (** Pause delivery for one partition. Local operation — no broker
      round-trip; safe to call from any fiber. [consume_partitioned] already
      uses this internally to stop its stream buffer from filling during an
      in-memory retry backoff sleep; exposed here for callers implementing
      their own retry or backpressure scheme outside [consume_partitioned]
      (e.g. a retry-topics consumer). *)

  val resume_partition : t -> topic:string -> partition:int32 -> (unit, Error.t) result
  (** Resume a partition paused with {!pause_partition}. *)

  (** Process messages until the handler returns [Stop], the consumer is
      closed, or the handler returns [Error _]. Closing the consumer stops the
      loop as [Ok ()]; handler errors are returned unchanged. *)
  val consume
    :  t
    -> ?hooks:hooks
    -> ?stop:unit Eio.Promise.t
    -> handler:(message -> ack:(unit -> (unit, Error.t) result) -> 'e handler_result)
    -> unit
    -> (unit, 'e) result

  type retry_policy = {
    base_delay_s : float;
    max_delay_s  : float;
    max_attempts : int;
    jitter_ratio : float;
  }

  val default_retry : retry_policy
  val backoff_s : rng:Random.State.t -> retry_policy -> int -> float
  val default_queue_capacity : int

  type 'e consume_error =
    | Handler_errors of (int32 * 'e) list
    | Invalid_config of string

  val consume_partitioned
    :  t
    -> sw:Eio.Switch.t
    -> clock:_ Eio.Time.clock
    -> ?retry:retry_policy
    -> ?hooks:hooks
    -> ?queue_capacity:int
    -> ?stop:unit Eio.Promise.t
    -> handler:(message -> ack:(unit -> (unit, Error.t) result) -> 'e handler_result)
    -> unit
    -> (unit, 'e consume_error) result
end

module Producer : sig
  type delivery_mode =
    | At_least_once
    | At_most_once
    | Exactly_once of { transaction_id : string }

  type config = {
    brokers       : string list;
    delivery_mode : delivery_mode;
    linger_ms     : int option;
    security      : Security.t;
    properties    : (string * string) list;
  }

  type t

  type topic_config = { min_insync_replicas : int }

  val create : config -> sw:Eio.Switch.t -> (t, Error.t) result
  val close : t -> unit

  val create_topic_with_config
    :  t
    -> topic_name:string
    -> partitions:int
    -> replication_factor:int
    -> config:topic_config
    -> (unit, Error.t) result

  val create_topic
    :  t
    -> topic_name:string
    -> partitions:int
    -> replication_factor:int
    -> (unit, Error.t) result

  val produce
    :  t
    -> topic:string
    -> value:bytes option
    -> ?key:bytes
    -> ?headers:(string * string option) list
    -> unit
    -> (unit, Error.t) result

  val produce_await
    :  t
    -> topic:string
    -> value:bytes option
    -> ?key:bytes
    -> ?headers:(string * string option) list
    -> unit
    -> (unit, Error.t) result Eio.Promise.t

  val flush : t -> timeout_ms:int -> (unit, Error.t) result

  type txn_failure = {
    error          : Error.t;
    is_fatal       : bool;
    is_retriable   : bool;
    requires_abort : bool;
    abort_error    : Error.t option;
        (** [None] when [requires_abort] is [false], or when the required
            abort itself succeeded. [Some _] when [with_transaction]'s
            recovery abort failed — a caller retrying based on
            [is_fatal = false] could otherwise hit a confusing "invalid
            state" error on the next transaction with no link back to
            this being the real cause. *)
  }

  type transaction_error =
    | App_error of { error : Error.t; abort_error : Error.t option }
        (** [f] returned [Error error]. [with_transaction] always attempts an
            abort in this case; [abort_error] is [Some _] only when that
            recovery abort itself failed (also reported via [with_transaction]'s
            [on_warning], default stderr). When [f] raises instead, the
            original exception propagates unwrapped — an abort is still
            attempted, but its outcome can only be reported via [on_warning]. *)
    | Txn_failure of txn_failure

  val string_of_transaction_error : transaction_error -> string

  val with_transaction
    :  t
    -> ?consumer_offsets:(Consumer.t * (string * int32 * int64) list)
    -> ?on_warning:(string -> unit)
    -> (unit -> (unit, Error.t) result)
    -> (unit, transaction_error) result
end
