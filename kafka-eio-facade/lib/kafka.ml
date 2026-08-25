(** [Kafka] is the canonical entry point: [open Kafka] and use
    [Kafka.Producer]/[Kafka.Consumer]/[Kafka.Error]/[Kafka.Security]
    instead of the flat [Kafka_producer]/[Kafka_consumer]/[Kafka_error]/
    [Kafka_security] module names those packages still export directly
    (kept for existing callers — this is a pure alias, not a rename).
    [Kafka_raw] is deliberately not aliased here: it is internal to
    kafka-eio-core and was never meant to be part of the public surface. *)

module Producer = Kafka_producer
module Consumer = Kafka_consumer
module Error = Kafka_error
module Security = Kafka_security
