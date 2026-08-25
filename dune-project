(lang dune 3.0)
(name kafka-eio)
(version 0.1.0)
(generate_opam_files true)

(package
 (name kafka-eio)
 (synopsis "Eio-native Kafka client for OCaml 5, built on librdkafka")
 (description "\
Producer and consumer libraries for Kafka on Eio/OCaml 5, backed by librdkafka.
Idempotent/transactional delivery, consumer-group streaming with explicit ack,
partition-aware concurrency, and typed transport security (plaintext/SSL/SASL).")
 (maintainers "lbendtlynielsen@gmail.com")
 (authors "lbendtly")
 (license "MIT")
 (homepage "https://github.com/loganbnielsen/kafka-eio")
 (bug_reports "https://github.com/loganbnielsen/kafka-eio/issues")
 (source (github loganbnielsen/kafka-eio))
 (depends
  (ocaml (>= 5.4.0))
  (dune (>= 3.0))
  (eio (>= 1.3))
  (eio_main (>= 1.3))
  (cstruct (>= 6.0.0))
  (alcotest :with-test)))
