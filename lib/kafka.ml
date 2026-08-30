(** [Kafka] is the sole public entry point: use [Kafka.Producer]/
    [Kafka.Consumer]/[Kafka.Error]/[Kafka.Security]. The flat
    [Kafka_producer]/[Kafka_consumer]/[Kafka_error]/[Kafka_security]/
    [Kafka_raw] modules are implementation detail ([private_modules] in
    [lib/dune]) and are not visible to callers outside this library. *)

module Error = Kafka_error
module Security = Kafka_security

module Consumer = Kafka_consumer

module Producer = struct
  include Kafka_producer

  let with_transaction t ?consumer_offsets f =
    let consumer_offsets =
      Option.map
        (fun (consumer, offsets) -> (Kafka_consumer.handle consumer, offsets))
        consumer_offsets
    in
    Kafka_producer.with_transaction t ?consumer_offsets f
end
