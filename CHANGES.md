# Changes

## 0.2.0

- Public modules now ship as one `kafka-eio` library. The raw librdkafka FFI
  module is private; supported caller-facing modules are `Kafka.Producer`,
  `Kafka.Consumer`, `Kafka.Error`, `Kafka.Security`, and `Kafka`.
- `Kafka.Security` exposes librdkafka key/value `settings` instead of an
  `apply` function over raw Kafka config values.
- Transactional consumer offsets now use `Kafka.Producer.consumer_handle`, an
  opaque token returned by `Kafka.Consumer.handle`, without exposing raw
  librdkafka handles.
- `Kafka.Producer.produce_await` delivery receipts are no longer lossy under
  pipe backpressure. The C delivery callback now queues receipts in native
  memory and uses the pipe only as a wakeup, so a full pipe cannot strand an
  awaiting promise until shutdown.
- Fixed a `produce_await` deadlock introduced earlier in this same release:
  the delivery-callback rework above moved to a 1-byte wake pipe, but the
  reader still waited for a full 4096-byte buffer before returning.
- `Kafka.Consumer.stream` is no longer part of the public `Kafka` facade; callers
  should use the read-only `fetch`, `poll`, or `consume` APIs instead of receiving
  the writable internal queue.
- `Kafka.Consumer.consume` now stops normally when a directly closed consumer was
  blocked waiting for the next message.
- **API change**: `Kafka.Consumer.consume_partitioned` now returns exhausted
  handler failures as `Handler_errors (partition, error) list`, so callers can
  see which partition failed instead of receiving a scheduler-dependent single
  error. It still reports invalid consumer-loop configuration as
  `Invalid_config _`.
- Renamed a local `rc` binding in `produce_await` to `send_result` — the abbreviation
  read ambiguously next to Kafka's own return-code conventions. No public API change.
- Comment-only pass over the C stubs and consumer/producer: multi-paragraph rationale
  and "regression note"/bug-number framing collapsed to one or two sentences. No logic
  changes.

## 0.1.1

- Add `/usr/local` and Homebrew C include/library paths for librdkafka on FreeBSD and
  macOS.
- Skip the `/proc/self/fd` fd-leak regression test on platforms without procfs.

## 0.1.0

- Initial standalone OPAM package: `Kafka.Producer` and `Kafka.Consumer` on top of
  librdkafka, extracted from the Sun platform after real in-tree usage. Idempotent/
  transactional delivery, consumer-group streaming with explicit ack, partition-aware
  concurrency, and typed transport security (`Kafka.Security.t`: plaintext/SSL/SASL) on
  every producer, consumer, and service config.
- Delivery receipts flow through a Unix pipe from the C delivery callback (thread-safe,
  no OCaml runtime needed from C); a background Eio fiber reads the pipe and resolves
  pending promises.
- Blocking C calls (`ocaml_rd_kafka_flush`, `ocaml_rd_kafka_consumer_close`,
  `ocaml_rd_kafka_consumer_poll`) release the OCaml domain lock at the FFI boundary
  around the blocking librdkafka call, so the GC can run and Eio's cancellation delivers
  cleanly once the call returns — not worked around with `Eio_unix.run_in_systhread` or
  polling loops at the OCaml level.

### Post-tag fix (#1): mutex-unlock skipped under Eio cancellation

Every `Mutex.lock t.mutex; ...; Mutex.unlock t.mutex` pair in `Kafka.Producer` (delivery
callback dispatch, pending-promise bookkeeping in `produce_await`, and cleanup in
`close`) and the `poll_exit_r` resolution in `Kafka.Consumer`'s `poll_fiber` skipped the
unlock/resolve step if an `Eio.Cancel.Cancelled` (or any other exception) was raised
between the lock and unlock — leaving the mutex held forever, or a fiber awaiting
`poll_exit_r` hanging forever after `close`. Fixed by wrapping every such critical
section in `Fun.protect ~finally`, so the unlock/resolve always runs regardless of how
the protected code exits.
