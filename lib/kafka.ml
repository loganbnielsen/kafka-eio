(** [Kafka] is the entry point: [Kafka.Consumer], [Kafka.Producer], [Kafka.Error],
    [Kafka.Security], [Kafka.Topic_name]. Each one is the module that implements
    it ([Kafka_consumer], [Kafka_producer], ...), so every interface exists once
    and nothing has to keep two copies of it in step.

    The flat modules are public as well: [Kafka_raw] is the librdkafka binding
    layer and [Kafka_consumer_handle] the offset token that
    {!Kafka_producer.with_transaction} takes, each documented where it is
    declared. A consumer's token comes from {!Kafka_consumer.handle}. *)

module Error = Kafka_error
module Security = Kafka_security
module Topic_name = Kafka_topic_name
module Consumer = Kafka_consumer
module Producer = Kafka_producer
