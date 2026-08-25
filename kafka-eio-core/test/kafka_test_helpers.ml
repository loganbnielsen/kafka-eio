let brokers = Kafka_test_brokers.brokers

let default_producer_config () : Kafka_producer.config =
  { brokers = brokers ()
  ; delivery_mode = Kafka_producer.At_least_once
  ; linger_ms = None
  ; security = Kafka_security.default
  ; properties = []
  }

let default_consumer_config ~group_id ~topics () : Kafka_consumer.config =
  { brokers = brokers ()
  ; group_id
  ; topics
  ; offset_reset = Kafka_consumer.Earliest
  ; auto_commit = false
  ; security = Kafka_security.default
  ; properties = []
  }
