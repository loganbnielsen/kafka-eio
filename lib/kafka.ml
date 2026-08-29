(** [Kafka] is the sole public entry point: use [Kafka.Producer]/
    [Kafka.Consumer]/[Kafka.Error]/[Kafka.Security]. The flat
    [Kafka_producer]/[Kafka_consumer]/[Kafka_error]/[Kafka_security]/
    [Kafka_raw] modules are implementation detail ([private_modules] in
    [lib/dune]) and are not visible to callers outside this library. *)

module Producer = Kafka_producer
module Consumer = Kafka_consumer
module Error = Kafka_error
module Security = Kafka_security
