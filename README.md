# kafka-eio

Eio-native Kafka client for OCaml 5, built on librdkafka.

- `kafka-eio-core` — internal shared FFI/error/security layer, installed as
  `kafka-eio.core` for the higher-level packages; `Kafka_raw` is unstable and
  not the supported user API
- `kafka-eio-producer` — producer: fire-and-forget, awaitable delivery, transactions
- `kafka-eio-consumer` — consumer: fetch/stream/poll, consumer groups, explicit ack, partition-aware retry
- `kafka-eio-facade` (`Kafka` module, findlib `kafka-eio.kafka`) — thin aliases
  (`Kafka.Producer`, `Kafka.Consumer`, `Kafka.Error`, `Kafka.Security`) over the
  three packages above, for `open Kafka` instead of the flat module names
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

See each package's `<name>.md` for its design document.
