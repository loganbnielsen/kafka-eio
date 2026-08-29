let brokers = Kafka_test_brokers.brokers

let default_producer_config () : Kafka.Producer.config =
  { brokers = brokers ()
  ; delivery_mode = Kafka.Producer.At_least_once
  ; linger_ms = None
  ; security = Kafka.Security.default
  ; properties = []
  }

let default_consumer_config ~group_id ~topics () : Kafka.Consumer.config =
  { brokers = brokers ()
  ; group_id
  ; topics
  ; offset_reset = Kafka.Consumer.Earliest
  ; auto_commit = false
  ; security = Kafka.Security.default
  ; properties = []
  }
