# kafka-eio

Eio-native Kafka client for OCaml 5, built on librdkafka.

- `Kafka_producer` — fire-and-forget, awaitable delivery, transactions
- `Kafka_consumer` — fetch/stream/poll, consumer groups, explicit ack, partition-aware retry
- `Kafka_error` and `Kafka_security` — shared error and transport-security contracts
- `Kafka` — thin aliases (`Kafka.Producer`, `Kafka.Consumer`, `Kafka.Error`,
  `Kafka.Security`) for callers that prefer one namespace
- `demo/` — minimal produce-then-consume sandbox binary

Extracted from the [Sun](https://github.com/loganbnielsen/sun) platform, where
`kafka-eio-service` (typed message contracts, schema registry, Redpanda admin,
retry topics/DLQ — the opinionated application layer) still lives and now
depends on this as an external library.

## Build

```bash
eval $(opam env)
dune build
```

Prerequisite: `sudo apt-get install -y librdkafka-dev`

## Test

```bash
# Unit tests (no broker needed)
dune runtest

# Integration tests (requires a running broker)
KAFKA_BROKERS=localhost:9092 dune build @runtest-integration
```

The unsafe librdkafka FFI layer is private to the package; application code uses
the modules above.
