(** Unsafe FFI bindings to librdkafka. Internal to kafka-eio-core.
    Users interact with Kafka_producer and Kafka_consumer, not this module. *)

type kafka_handle
type kafka_conf
type kafka_topic
type kafka_type = Producer | Consumer

val conf_new : unit -> kafka_conf
val conf_set : kafka_conf -> string -> string -> (unit, string) result

(** [kafka_new typ conf write_fd] creates a kafka handle. For Producer handles,
    [write_fd] is the write end of a pipe used for delivery notifications.
    Pass [(-1)] for Consumer handles (delivery callback not installed). *)
val kafka_new : kafka_type -> kafka_conf -> int -> (kafka_handle, string) result

(** [topic_new handle name] returns [Error msg] for an invalid topic name
    rather than a null handle — [rd_kafka_topic_new] can fail (invalid name,
    invalid topic-level config), and a null pointer stored in the returned
    custom block would crash a later [produce] on that topic. *)
val topic_new : kafka_handle -> string -> (kafka_topic, string) result

(** [topic_destroy topic] releases [topic]'s local state. Nulls the
    internal pointer first, so the GC finalizer becomes a no-op —
    double-destroy is impossible, matching [destroy] for [kafka_handle].
    librdkafka's documented lifecycle contract requires every topic
    object to be destroyed before the handle that created it; callers
    that cache [kafka_topic] values must call this on each before
    calling [destroy] on the owning handle. *)
val topic_destroy : kafka_topic -> unit

(** [produce topic partition value_opt key_opt correlation_id]
    Enqueues a message. [correlation_id = 0L] means fire-and-forget.
    Non-zero correlation ids are written back to the delivery pipe on ack.
    [value_opt = None] sends a Kafka tombstone (a NULL payload — the
    delete marker for a key on a compacted topic), distinct from
    [Some Bytes.empty], a genuine zero-length value. *)
val produce
  :  kafka_topic
  -> int32
  -> bytes option
  -> bytes option
  -> int64
  -> (unit, int) result

(** [enable_queue_events handle write_fd] registers [write_fd] with the
    librdkafka main queue.  One byte is written to [write_fd] whenever the
    queue transitions from empty to non-empty.  Call [poll handle 0] after
    waking on the matching read end to drain all pending events. *)
val enable_queue_events : kafka_handle -> int -> unit

(** [disable_queue_events handle] clears the io-event callback registered by
    [enable_queue_events].  Call during producer shutdown before closing the
    pipe so librdkafka cannot write to a recycled file descriptor. *)
val disable_queue_events : kafka_handle -> unit

val poll    : kafka_handle -> int -> int
val flush   : kafka_handle -> int -> (unit, int) result

(** [destroy handle] flushes with 0ms timeout, nulls the internal pointer, and
    calls rd_kafka_destroy with the OCaml domain lock released. After this call
    the GC finalizer for [handle] becomes a no-op — double-destroy is impossible. *)
val destroy : kafka_handle -> unit
val err2str : int -> string

(** Consumer-specific *)
val subscribe         : kafka_handle -> string list -> (unit, string) result

(** Outcome of one [consumer_poll]. [Poll_error] carries a raw librdkafka
    error code — auth failure, unknown topic, max-poll-interval exceeded, a
    lost/revoked assignment, etc. [RD_KAFKA_RESP_ERR__PARTITION_EOF] is
    reported as [Timeout], since it is informational rather than an error.

    In [Msg], the value is [bytes option] and header values are
    [string option] rather than bare [bytes]/[string]: a Kafka tombstone
    (a NULL payload, used to delete a key on a compacted topic) and a
    NULL-valued header must stay distinguishable from a real zero-length
    value — collapsing both to "" would be silent data loss. *)
type poll_result =
  | Timeout
  | Msg of (string * int32 * int64 * bytes option * bytes option * int64 option * (string * string option) list)
  | Poll_error of int

val consumer_poll     : kafka_handle -> int -> poll_result

(** [consumer_queue_events_enable handle write_fd] registers [write_fd] with
    the consumer queue, distinct from the main queue used by producers. Call
    [consumer_queue_poll handle 0] after waking on the matching read end. *)
val consumer_queue_events_enable : kafka_handle -> int -> unit

(** [consumer_queue_events_disable handle] clears the io-event callback
    registered by [consumer_queue_events_enable]. Call during consumer
    shutdown before closing the pipe so librdkafka cannot write to a recycled
    file descriptor. *)
val consumer_queue_events_disable : kafka_handle -> unit

(** [consumer_queue_poll handle timeout_ms] reads at most one message from the
    consumer queue via [rd_kafka_consume_queue], with [consumer_poll]'s result
    shape and error semantics. *)
val consumer_queue_poll : kafka_handle -> int -> poll_result

(** [produce_v handle topic_name partition value_opt key_opt correlation_id headers]
    Enqueues a message using rd_kafka_producev, supporting Kafka message headers.
    [headers] is transferred to librdkafka on success. [correlation_id = 0L] means
    fire-and-forget. Use when header propagation (e.g. traceparent) is needed.
    [value_opt = None] sends a tombstone, as in [produce]. A header's
    [None] value sends a NULL-valued header, distinct from [Some ""] —
    mirroring [Kafka_consumer.message.headers] on the read side. *)
val produce_v
  :  kafka_handle
  -> string                            (* topic name *)
  -> int32                             (* partition; -1 = auto *)
  -> bytes option                      (* value; None = tombstone *)
  -> bytes option                      (* key *)
  -> int64                             (* correlation id *)
  -> (string * string option) list     (* headers *)
  -> (unit, int) result
val consumer_close    : kafka_handle -> unit

(** Returns the number of partitions currently assigned to this consumer.
    Fast local query — does not block or call the broker. *)
val assignment_count  : kafka_handle -> int

(** Returns the currently assigned [(topic, partition)] pairs.
    Fast local query — does not block or call the broker. *)
val assignment : kafka_handle -> (string * int32) list

(** [create_topic handle ~topic_name ~partitions ~replication_factor]
    creates a topic via librdkafka's admin API on an existing handle.
    Releases the OCaml domain lock while awaiting the broker response.
    Returns 0 on success; treats TOPIC_ALREADY_EXISTS as success.
    Returns a non-zero librdkafka error code on failure. *)
val create_topic : kafka_handle -> topic_name:string -> partitions:int -> replication_factor:int -> int

(** Commit one explicit offset. Committed offset is [offset + 1] (the next
    offset to fetch), matching Kafka's own convention. *)
val commit_message : kafka_handle -> topic:string -> partition:int32 -> offset:int64 -> async:bool -> (unit, int) result

(** Commit the consumer's entire current assignment. A separate call from
    [commit_message] rather than an empty-topic sentinel, so an empty topic
    name can never be misread as "commit everything". Note: this commits
    each assigned partition's current *fetch position*, not what the
    application actually processed — [commit_offsets] is the safe
    alternative when the caller tracks explicit per-partition offsets. *)
val commit_all : kafka_handle -> bool -> (unit, int) result

(** [commit_offsets handle offsets async] commits explicit
    [(topic, partition, last-processed-offset)] tuples in one call, each
    committed as offset+1 like [commit_message]. An empty list commits
    nothing and succeeds trivially. Unlike [commit_all], this only ever
    commits offsets the caller explicitly names. *)
val commit_offsets
  :  kafka_handle
  -> (string * int32 * int64) list
  -> bool
  -> (unit, int) result

(** Delivery pipe *)
val pipe_create     : unit -> int * int
val delivery_sizeof : unit -> int
val read_delivery   : int -> int64 * int

(** Transactional API.

    Every transactional call can fail with more than a bare error code:
    librdkafka reports, per error instance (not derivable from the code
    alone), whether the producer must abort, whether the failure is fatal
    to the whole producer, and whether it is safe to retry. *)
type txn_error = {
  code           : int;
  is_fatal       : bool;
  is_retriable   : bool;
  requires_abort : bool;
}

val init_transactions  : kafka_handle -> int -> (unit, txn_error) result
val begin_transaction  : kafka_handle -> (unit, txn_error) result
val commit_transaction : kafka_handle -> int -> (unit, txn_error) result
val abort_transaction  : kafka_handle -> int -> (unit, txn_error) result

(** [send_offsets_to_transaction producer consumer offsets timeout_ms] adds
    [offsets] — explicit (topic, partition, offset-of-last-processed-message)
    tuples — to the open transaction on [producer], using [consumer]'s group
    metadata for fencing. Each offset is committed as offset+1, matching
    [commit_message]'s convention. Unlike committing the consumer's current
    assignment/position, this only ever advances exactly the offsets the
    caller says it processed inside the transaction. *)
val send_offsets_to_transaction
  :  kafka_handle
  -> kafka_handle
  -> (string * int32 * int64) list
  -> int
  -> (unit, txn_error) result

(** [pause_partition handle topic partition] pauses delivery for one partition.
    Local operation — no broker round-trip. Safe to call from any fiber. *)
val pause_partition  : kafka_handle -> string -> int32 -> unit

(** [resume_partition handle topic partition] resumes delivery for one partition. *)
val resume_partition : kafka_handle -> string -> int32 -> unit
