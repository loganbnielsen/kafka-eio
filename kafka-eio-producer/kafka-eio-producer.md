# kafka-eio-producer — Design Document

## Overview

A modern OCaml Kafka producer library built on librdkafka and Eio. Part of a planned
two-library suite — producer and consumer are intentionally separate packages so users
can take only what they need.

Goals:
- Eio-native from day one (OCaml 5 effects-based concurrency)
- No exceptions as control flow — errors are values
- Type-safe, exhaustive error handling via variants
- librdkafka as the underlying transport (battle-tested protocol, C FFI)
- Modular enough to swap librdkafka for a pure OCaml implementation later

## Package Structure

```
kafka-eio-producer/
  lib/
    kafka_producer.ml   # public API
    kafka_producer.mli
  test/
    ...
```

`Kafka_raw` and `Kafka_error` are provided by `kafka-eio-core` and shared with
`kafka-eio-consumer`. The producer does not duplicate the FFI layer or C stubs.
See the core design doc for the full FFI surface, including the transactional API.

## Configuration

```ocaml
type delivery_mode =
  | At_least_once   (** default — idempotent producer, acks=all *)
  | At_most_once    (** fire and forget, acks=0 *)
  | Exactly_once of { transaction_id : string }  (** EOS, explicit opt-in *)

type config = {
  brokers       : string list;
  delivery_mode : delivery_mode;
  linger_ms     : int option;          (** batch window; None = librdkafka default (5 ms) *)
  security      : Kafka_security.t;    (** use [Kafka_security.default] for plaintext dev *)
  properties    : (string * string) list;
  (** raw librdkafka config keys applied after every field above — the
      escape hatch for options this module has no typed field for. *)
}
```

Config values map to librdkafka string key/value pairs at initialization time. The
typed config ensures users can't pass invalid combinations and steers them toward safe
defaults, while `properties` keeps the door open for anything typed fields don't cover.

## Producer Handle

```ocaml
type t  (* abstract — wraps kafka_handle + Eio resources *)

(** Starts delivery and poll fibers in [sw]; closes when [sw] ends. *)
val create : config -> sw:Eio.Switch.t -> (t, Kafka_error.t) result
val close  : t -> unit
```

## Producing Messages

Two variants — fire and forget vs. awaitable delivery receipt. Both take
an optional key and Kafka message headers; the trailing `unit` is required
by OCaml's optional-argument erasure rules:

```ocaml
val produce
  :  t -> topic:string -> value:bytes option
  -> ?key:bytes -> ?headers:(string * string option) list -> unit
  -> (unit, Kafka_error.t) result

val produce_await
  :  t -> topic:string -> value:bytes option
  -> ?key:bytes -> ?headers:(string * string option) list -> unit
  -> (unit, Kafka_error.t) result Eio.Promise.t

(** Wait for all enqueued messages to be delivered *)
val flush : t -> timeout_ms:int -> (unit, Kafka_error.t) result
```

`value` is `bytes option`, required rather than optional-with-a-default, so
a caller must explicitly choose: `value:(Some bytes)` for a real message,
`value:None` to send a Kafka tombstone (a NULL payload — the delete marker
for a key on a compacted topic). Making it required rather than defaulting
to `None` means a forgotten `~value` is a compile error, not a silently
sent tombstone. This mirrors `Kafka_consumer.message.value`, which is
`bytes option` for the same reason on the read side.

`~key`, if given, is used exactly as passed — including a genuine
zero-length key, which Kafka routes differently (a real, hashed key)
from omitting `~key` entirely (round-robin/sticky partitioning, since
there is no key at all). A header's value is `string option`: `None`
sends a NULL-valued header, distinct from `Some ""`, again mirroring the
consumer's `message.headers`.

Either function can return (or, for `produce_await`, resolve its promise
to) `Error Kafka_error.Queue_full` if librdkafka's local send queue is
full. Neither blocks or retries on your behalf — a caller producing at a
high rate must handle `Queue_full` itself (drop, retry after a delay, or
apply its own backpressure). There is currently no Eio-native blocking
backpressure variant; this is an accepted v0.1 scope cut, not an
oversight.

Every operation above returns `Error Kafka_error.Destroy` once `close` has
run, instead of touching the destroyed handle; `close` also resolves any
still-pending `produce_await` promises to `Error Kafka_error.Destroy` so a
fiber awaiting one can't hang past shutdown.

## Transactional API

When `delivery_mode = Exactly_once`, the producer exposes a transaction bracket:

```ocaml
(** A transactional-API call (begin/commit/abort/send-offsets) itself
    failed. [is_fatal]/[is_retriable]/[requires_abort] are librdkafka's
    per-error-instance flags, not derivable from [error] alone. *)
type txn_failure = {
  error          : Kafka_error.t;
  is_fatal       : bool;
  is_retriable   : bool;
  requires_abort : bool;
}

type transaction_error =
  | App_error of Kafka_error.t  (** [f] returned [Error _], or raised *)
  | Txn_failure of txn_failure  (** the transactional-API call itself failed *)

val string_of_transaction_error : transaction_error -> string

(** Runs [f] inside a Kafka transaction. Commits on [Ok], aborts on [Error]
    or exception. Requires [delivery_mode = Exactly_once]. *)
val with_transaction
  :  t
  -> ?consumer_offsets:(Kafka_consumer_handle.t * (string * int32 * int64) list)
  -> (unit -> (unit, Kafka_error.t) result)
  -> (unit, transaction_error) result
```

Internally this calls `init_transactions` once at producer creation, then
`begin_transaction` / `commit_transaction` / `abort_transaction` around the
user-supplied function, aborting only when librdkafka's `requires_abort`
flag says to. `?consumer_offsets` is `(consumer_handle, offsets)` where
`offsets` are the exact `(topic, partition, offset)` tuples `f` actually
processed from that consumer — built from the consumed messages'
`topic`/`partition`/`offset` fields, **not** the consumer's current
assignment or position (which can be ahead of what `f` processed if the
consumer's poll fiber prefetched further). See the core design doc for the
full transaction semantics.

## Admin: Creating Topics

```ocaml
(** Creates a topic via librdkafka's admin API, reusing this producer's
    handle. Treats an already-existing topic as success. *)
val create_topic
  :  t
  -> topic_name:string
  -> partitions:int
  -> replication_factor:int
  -> (unit, Kafka_error.t) result
```

There used to be a `raw_handle : t -> Kafka_raw.kafka_handle` accessor for
this — callers reached into `Kafka_raw.create_topic` themselves. `Kafka_raw`
is internal to `kafka-eio-core` and was never meant to be part of this
package's public surface, so `raw_handle` is gone; `create_topic` is the
replacement and the only admin operation exposed today. A real `Admin`
module (topic config, partition/replica changes, ...) is future work, not
built on top of a leaked raw handle.

## Delivery Receipt Implementation (Internal)

`produce_await` works by attaching an Eio promise resolver to each message via
librdkafka's `msg_opaque` void pointer:

```
produce_await
  -> create Eio promise + resolver
  -> store resolver pointer in msg_opaque
  -> call rd_kafka_produce
  -> return promise to caller

delivery callback fires (librdkafka thread)
  -> fish resolver out of msg_opaque
  -> resolve promise with Ok () or Error err
  -> caller's fiber unblocks
```

This is the core interesting engineering problem of the producer — safely passing an
OCaml value (the resolver) through a C void pointer and back.

## Example Usage

```ocaml
let () =
  Eio_main.run @@ fun _env ->
    Eio.Switch.run @@ fun sw ->
      let cfg : Kafka_producer.config = {
        brokers = ["localhost:9092"];
        delivery_mode = At_least_once;
        linger_ms = None;
        security = Kafka_security.default;
        properties = [];
      } in
      match Kafka_producer.create cfg ~sw with
      | Error e ->
        Printf.printf "Failed to create producer: %s\n" (Kafka_error.to_string e)
      | Ok producer ->
        (* fire and forget *)
        let _ = Kafka_producer.produce producer ~topic:"events" ~value:(Some (Bytes.of_string "hello")) () in

        (* await acknowledgment *)
        let receipt = Kafka_producer.produce_await producer ~topic:"events" ~value:(Some (Bytes.of_string "hello")) () in
        (match Eio.Promise.await receipt with
        | Ok ()   -> print_endline "delivered"
        | Error e -> Printf.printf "error: %s\n" (Kafka_error.to_string e))
```

## Future: Lwt Compatibility Shim

A thin `kafka-eio-producer-lwt` package will wrap the Eio API for use in Lwt
codebases via the eio-lwt bridge. Not in scope for v1.

## Future: Pure OCaml Transport

The `Kafka_raw` FFI layer in core is intentionally isolated behind an interface. A
future `kafka-eio-producer-native` could implement the same interface in pure OCaml
using Eio TCP, removing the librdkafka C dependency entirely. The public API would
remain unchanged.

## Out of Scope (v1)

- Schema Registry integration
- Broader Admin API (partition/replica changes, topic config, ...) —
  `create_topic` (above) is the one admin operation implemented so far
- Lwt shim
