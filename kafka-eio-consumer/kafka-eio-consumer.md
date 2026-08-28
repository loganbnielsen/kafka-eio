# kafka-eio-consumer — Design Document

## Overview

A modern OCaml Kafka consumer library built on librdkafka and Eio. Companion package
to `kafka-eio-producer` — intentionally separate so users can take only what they need.

Goals:
- Eio-native stream-based API — consumption feels like processing a sequence
- Consumer group support out of the box
- Explicit offset management with safe defaults (at-least-once)
- Exactly-once via transactional API in coordination with `kafka-eio-producer`
- librdkafka as the underlying transport
- Modular enough to swap for pure OCaml transport later

## Package Structure

```
kafka-eio-consumer/
  lib/
    kafka_consumer.ml     # public API
    kafka_consumer.mli
  test/
    ...
```

`Kafka_raw` and `Kafka_error` are provided by `kafka-eio-core` and shared with
`kafka-eio-producer`. The consumer does not duplicate the FFI layer or C stubs.
See the core design doc for the full FFI surface.

## Message Type

Defined by the Kafka protocol and exposed by librdkafka's `rd_kafka_message_t`.
The OCaml type is a clean projection:

```ocaml
type message = {
  topic     : string;
  partition : int32;
  offset    : int64;
  key       : bytes option;
  value     : bytes option;                    (* None = tombstone, distinct from Some "" *)
  timestamp : int64 option;
  headers   : (string * string option) list;   (* Kafka 0.11+; None value distinct from Some "" *)
}
```

Headers are the Kafka-native metadata mechanism — arbitrary key/value pairs attached
to a message at produce time. Timestamp comes from a separate librdkafka call
(`rd_kafka_message_timestamp`) and is optional since older brokers may not provide it.

`value` and header values are options rather than bare `bytes`/`string` so a Kafka
tombstone (a NULL payload — the delete marker for a key on a compacted topic) and a
NULL-valued header stay distinguishable from a genuine zero-length value. Collapsing
both to `""` at the FFI boundary would silently lose which one actually happened.

## Configuration

```ocaml
type offset_reset =
  | Earliest   (** start from beginning of partition on first read *)
  | Latest     (** start from end — only new messages *)

type config = {
  brokers      : string list;
  group_id     : string;                              (** required for consumer group coordination *)
  topics       : string list;
  offset_reset : offset_reset;
  auto_commit  : bool;                                (** default: false — prefer explicit ack *)
  security     : Kafka_security.t;                    (** use [Kafka_security.default] for plaintext dev *)
  properties   : (string * string) list;
  (** raw librdkafka config keys applied after every field above — the
      escape hatch for options this module has no typed field for. *)
}
```

`group_id` is required rather than optional — consumer groups are the standard
production pattern. Users who want manual partition assignment can set a unique
`group_id` per instance.

## Rebalancing (current limitation, pre-1.0)

`conf_of_config` pins `partition.assignment.strategy` to `range,roundrobin`
(eager rebalancing) rather than librdkafka's newer `cooperative-sticky`
default, and no rebalance callback is registered. This is deliberate, not
an oversight: librdkafka's cooperative protocol requires a rebalance
callback to drive its multi-round handshake, and without one the rebalance
never completes — a consumer would never receive an assignment at all.
Eager rebalancing works callback-free, at a real cost:

- Applications cannot observe partition assignment or revocation. There is
  no way to flush per-partition state, or to know a partition was lost,
  before it is reassigned to another consumer in the group.
- Eager rebalancing revokes the *entire* current assignment before
  reassigning, so every rebalance is a brief full stop for the whole
  consumer, not just the moved partitions.

This is acceptable for a single-consumer-per-group deployment, or for
consumers whose processing is idempotent per message regardless of which
instance holds a partition. It is not yet suitable for stateful per-
partition processing that must react to losing a partition. There is no
`on_rebalance`/rebalance-event API — do not depend on one existing.
A two-consumer rebalance integration test covers `commit_all` not rolling
back offsets across revoke/reassign. Before 1.0, decide whether to expose
`Assigned`/`Revoked`/`Lost` events (which requires registering a
librdkafka rebalance callback and taking on responsibility for calling
assign/unassign, per librdkafka's contract).

## Polling Model

Both producer and consumer poll fibers are event-driven. The producer
watches librdkafka's main queue and drains `rd_kafka_poll` after wakeup;
the consumer watches `rd_kafka_queue_get_consumer` and drains
`rd_kafka_consume_queue(queue, 0)`. Neither fiber blocks its Eio scheduler
thread while idle.

The consumer used to call `Kafka_raw.consumer_poll t.handle 100` in a loop.
That released the OCaml runtime lock, but still left the Eio scheduler's OS
thread inside a blocking C call. `sun` reproduced this with `Service.run`
and `Worker.Make(W).run` in one process: HTTP requests stopped being read
while the worker consumer was alive. Moving the consumer to queue wakeups
removes that class of scheduler interference.

A bare kafka-eio-only repro has not been found yet: isolated attempts with
`Kafka_consumer.t`, `Kafka_producer.t`, and a plain `Cohttp_eio.Server`
responded correctly even with the old blocking implementation. Until a
smaller repro exists, `sun`'s `examples/local-demo` e2e test is the
regression signal for this bug.

Pure rebalances still wake the consumer queue. This was checked with two
consumers and no produced messages; assignment is therefore tracked per
wakeup instead of by a fixed 100ms poll cadence.

## Consumer Handle

```ocaml
type t  (* abstract — wraps kafka_handle + Eio resources *)

(** [on_ready] fires exactly once when the broker assigns this consumer
    partitions — use it to signal readiness instead of sleeping a fixed
    rebalance timeout. [on_poll_error] receives a raw librdkafka error code
    for any message-level poll error (auth failure, unknown topic,
    max-poll-interval exceeded, ...) other than end-of-partition; defaults
    to logging to stderr. *)
val create
  :  ?on_ready:(unit -> unit)
  -> ?on_poll_error:(int -> unit)
  -> config -> sw:Eio.Switch.t -> (t, Kafka_error.t) result

val close : t -> unit
```

Every operation below returns `Error Kafka_error.Destroy` once `close` has
run, instead of touching the destroyed handle.

`create` registers `close` with the supplied switch, so leaving the switch
scope closes the consumer even if the application did not call `close`
explicitly. The internal poll fiber is a daemon fiber for that reason:
it must not keep the switch alive while waiting for the release hook that
closes it.

## Core Consumption API

### Direct-Style: Fetch

The plain way to consume one message at a time:

```ocaml
(** Blocks until the next message is available and returns it. The
    direct-style equivalent of [Eio.Stream.take (stream t)]. *)
val fetch : t -> (message, Kafka_error.t) result
```

Usage:

```ocaml
let rec loop () =
  match Kafka_consumer.fetch consumer with
  | Error e -> Printf.eprintf "fetch failed: %s\n%!" (Kafka_error.to_string e)
  | Ok msg -> process msg; loop ()
in
loop ()
```

### Low-Level: Stream

`fetch` is sugar over the underlying Eio stream, exposed directly for
callers who need other `Eio.Stream` operations (e.g. `take_nonblocking`,
or composing with `Eio.Fiber.first`):

```ocaml
(** Returns a stream of incoming messages. Runs the librdkafka poll loop
    in a background fiber. Backpressure is handled via Eio.Stream capacity. *)
val stream : t -> message Eio.Stream.t
```

### Non-Blocking: Poll

For a non-blocking check instead of `fetch`'s/`stream`'s blocking take:

```ocaml
(** Non-blocking check for one message. Returns [None] immediately if the
    queue is empty. Reads from the same internal stream [stream] does, so
    it never competes with it for the same underlying librdkafka poll. *)
val poll : t -> (message option, Kafka_error.t) result
```

## Offset Management

### Explicit Ack (recommended)

The handler receives an `ack` function alongside the message. Calling `ack ()` commits
the offset and returns that commit's own result — a synchronous librdkafka call that
can itself fail. Handlers report outcome via `'e handler_result`, polymorphic in the
application's own error type since most handler errors (decode, database, HTTP, ...)
have nothing to do with Kafka:

```ocaml
type 'e handler_result =
  | Continue
  | Stop
  | Error of 'e

(** [on_warning] receives a human-readable message for API-misuse events
    (e.g. a handler returning without calling [ack ()]); defaults to
    stderr. Returns [Ok ()] when [handler] returns [Stop], [Error e] when
    [handler] returns [Error e]. *)
val consume
  :  t
  -> ?on_warning:(string -> unit)
  -> handler:(message -> ack:(unit -> (unit, Kafka_error.t) result) -> 'e handler_result)
  -> unit
  -> (unit, 'e) result
```

The standard at-least-once pattern:

```ocaml
Kafka_consumer.consume consumer ~handler:(fun msg ~ack ->
  match write_to_database msg.value with
  | Error e -> Kafka_consumer.Error e
  | Ok () ->
    ignore (ack ());   (* only advance offset after successful DB write *)
    Kafka_consumer.Continue
) ()
```

### Auto-Commit

Available when `auto_commit = true` in config. librdkafka commits periodically in the
background. Simpler but risks message loss on crash — offset may advance before
processing completes.

### Manual Commit

For power users who need fine-grained control outside the `consume` abstraction:

```ocaml
(** Commit offset for a specific message explicitly *)
val commit : t -> message -> (unit, Kafka_error.t) result

(** Commit all currently assigned partitions' offsets *)
val commit_all : t -> (unit, Kafka_error.t) result
```

With `auto_commit = false` (the default), `commit_all` commits each
partition's last explicitly `commit`/`ack`'d offset — not whatever was
merely fetched into `stream`/`fetch`/`poll`. This matters: librdkafka's
own "commit the current assignment" behavior (what `commit_all` would
otherwise delegate to) tracks *fetch position*, not what the application
processed, so relying on it directly could silently commit past messages
the application never saw — the same class of bug fixed for `with_transaction`'s
explicit offsets. With `auto_commit = true`, `commit_all` instead commits
each partition's last *fetched* offset, carrying the same risk
`auto_commit` itself already documents (offset can advance before
processing completes).

### Per-Partition Fibers With Retry: `consume_partitioned`

For handlers that need per-message retry with backoff, `consume_partitioned`
routes each message to a dedicated fiber per partition, so one partition's
retry sleep never blocks another:

```ocaml
type retry_policy = {
  base_delay_s : float;  (** doubles on each consecutive failure *)
  max_delay_s  : float;  (** default: 600.0 *)
  max_attempts : int;    (** negative = retry indefinitely; default: -1 *)
}

val default_retry : retry_policy
val default_queue_capacity : int  (** 16 *)

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
```

During a partition's retry sleep, that partition is paused at the
librdkafka level (no new messages accumulate for it) while other
partitions keep flowing. Processing within one partition is strictly
sequential — do not process two messages from the same partition
concurrently against one `t`, since `ack` commits by offset and
out-of-order acknowledgement would silently skip unprocessed messages.
`queue_capacity` bounds each partition's in-memory queue; routing to a
full queue happens synchronously from the routing loop (a deliberate
choice — forking a fiber per message instead would trade a bounded stall
for unbounded fiber/message growth on a backed-up partition).

## Transactions (Exactly Once)

Exactly-once processing requires coordination between consumer and producer — the
consumer offset advance is tied atomically to the producer transaction. This is
handled on the producer side via `Kafka_producer.with_transaction ?consumer_offsets`.

From the consumer's perspective: pass `handle consumer` and the exact
`(topic, partition, offset)` tuples of the messages processed inside `f` —
not the whole consumer, and not its current assignment/position, since
either could include messages `f` never actually processed:

```ocaml
(* Consume-transform-produce, exactly once *)
match Kafka_consumer.poll consumer with
| Error e -> Error (Kafka_producer.App_error e)
| Ok None -> Ok ()
| Ok (Some m) ->
  let transformed = transform m.value in
  Kafka_producer.with_transaction producer
    ~consumer_offsets:(Kafka_consumer.handle consumer, [ (m.topic, m.partition, m.offset) ])
    (fun () ->
       Kafka_producer.produce_await producer ~topic:"output" ~value:transformed ()
       |> Eio.Promise.await)
```

The consumer offset only advances if the transaction commits. If the process crashes
mid-transaction, the broker aborts it and the message is replayed.

**Note:** transactions expect `auto_commit = false` in the consumer config —
combining a transactional producer with an auto-committing consumer isn't
useful (the auto-commit races the transaction's own offset commit) but
nothing currently enforces this; set it deliberately.

## Example: Full Consumer

```ocaml
let () =
  Eio_main.run @@ fun _env ->
    let cfg : Kafka_consumer.config = {
      brokers      = ["localhost:9092"];
      group_id     = "my-service";
      topics       = ["events"];
      offset_reset = Latest;
      auto_commit  = false;
      security     = Kafka_security.default;
      properties   = [];
    } in
    Eio.Switch.run @@ fun sw ->
    match Kafka_consumer.create cfg ~sw with
    | Error e ->
      Printf.printf "Failed: %s\n" (Kafka_error.to_string e)
    | Ok consumer ->
      ignore (Kafka_consumer.consume consumer ~handler:(fun msg ~ack ->
        Printf.printf "Got: %s\n"
          (match msg.value with Some v -> Bytes.to_string v | None -> "<tombstone>");
        ignore (ack ());
        Kafka_consumer.Continue
      ) ())
```

## Future: Lwt Compatibility Shim

A thin `kafka-eio-consumer-lwt` package will wrap the Eio API for Lwt codebases
via the eio-lwt bridge. Not in scope for v1.

## Future: Pure OCaml Transport

Same as the producer — `Kafka_raw` in core is isolated behind an interface. A future
`kafka-eio-consumer-native` could replace librdkafka with a pure OCaml Eio TCP
implementation. The public API would remain unchanged.

## Out of Scope (v1)

- Schema Registry integration
- Admin API
- Lwt shim
- Seek / manual partition assignment API (beyond `group_id` workaround)
