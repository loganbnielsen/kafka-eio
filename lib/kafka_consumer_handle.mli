type t = private Kafka_raw.kafka_handle

val of_raw : Kafka_raw.kafka_handle -> t
val to_raw : t -> Kafka_raw.kafka_handle
