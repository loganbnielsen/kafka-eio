(** Opaque consumer handle type shared between kafka-eio-core and kafka-eio-producer.
    This exists solely so kafka-eio-producer can accept a consumer handle in
    [with_transaction] without directly depending on kafka-eio-consumer. *)

type t

val of_raw : Kafka_raw.kafka_handle -> t
val to_raw : t -> Kafka_raw.kafka_handle
