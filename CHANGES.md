# Changes

## 0.1.1

- Add `/usr/local` and Homebrew C include/library paths for librdkafka on FreeBSD and
  macOS.
- Skip the `/proc/self/fd` fd-leak regression test on platforms without procfs.

## 0.1.0

- Initial standalone OPAM package: `Kafka_producer` and `Kafka_consumer` on top of
  librdkafka, extracted from the Sun platform after real in-tree usage. Idempotent/
  transactional delivery, consumer-group streaming with explicit ack, partition-aware
  concurrency, and typed transport security (`Kafka_security.t`: plaintext/SSL/SASL) on
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

Every `Mutex.lock t.mutex; ...; Mutex.unlock t.mutex` pair in `Kafka_producer` (delivery
callback dispatch, pending-promise bookkeeping in `produce_await`, and cleanup in
`close`) and the `poll_exit_r` resolution in `Kafka_consumer`'s `poll_fiber` skipped the
unlock/resolve step if an `Eio.Cancel.Cancelled` (or any other exception) was raised
between the lock and unlock — leaving the mutex held forever, or a fiber awaiting
`poll_exit_r` hanging forever after `close`. Fixed by wrapping every such critical
section in `Fun.protect ~finally`, so the unlock/resolve always runs regardless of how
the protected code exits.
