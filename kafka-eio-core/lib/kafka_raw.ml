type kafka_handle
type kafka_conf
type kafka_topic
type kafka_type = Producer | Consumer

external conf_new : unit -> kafka_conf
  = "ocaml_rd_kafka_conf_new"

external conf_set : kafka_conf -> string -> string -> (unit, string) result
  = "ocaml_rd_kafka_conf_set"

external kafka_new : kafka_type -> kafka_conf -> int -> (kafka_handle, string) result
  = "ocaml_rd_kafka_new"

external topic_new : kafka_handle -> string -> (kafka_topic, string) result
  = "ocaml_rd_kafka_topic_new"

external topic_destroy : kafka_topic -> unit = "ocaml_rd_kafka_topic_destroy"

external produce
  :  kafka_topic -> int32 -> bytes option -> bytes option -> int64
  -> (unit, int) result
  = "ocaml_rd_kafka_produce"

external enable_queue_events : kafka_handle -> int -> unit
  = "ocaml_kafka_enable_queue_events"

external disable_queue_events : kafka_handle -> unit
  = "ocaml_kafka_disable_queue_events"

external poll : kafka_handle -> int -> int
  = "ocaml_rd_kafka_poll"

external flush : kafka_handle -> int -> (unit, int) result
  = "ocaml_rd_kafka_flush"

(* destroy nulls the OCaml pointer before calling rd_kafka_destroy, so the GC
   finalizer becomes a no-op. Releases the domain lock during the blocking call. *)
external destroy : kafka_handle -> unit = "ocaml_rd_kafka_destroy"
let () = ignore destroy   (* suppress warning 32 — used by kafka_producer *)


external err2str : int -> string
  = "ocaml_rd_kafka_err2str"

external subscribe : kafka_handle -> string list -> (unit, string) result
  = "ocaml_rd_kafka_subscribe"

type poll_result =
  | Timeout
  | Msg of (string * int32 * int64 * bytes option * bytes option * int64 option * (string * string option) list)
  | Poll_error of int

external consumer_poll : kafka_handle -> int -> poll_result
  = "ocaml_rd_kafka_consumer_poll"

(* Event-driven alternative to consumer_poll: consumer_queue_events_enable
   registers write_fd to receive a wake byte whenever the consumer queue
   (rd_kafka_queue_get_consumer — distinct from the main queue enable_queue_events
   watches, which never carries consumer messages) transitions empty -> non-empty.
   consumer_queue_poll then drains with timeout_ms=0 on wake, so no call in this
   pair ever blocks the calling thread for a real duration — unlike consumer_poll,
   whose 100ms blocking call sits inside a foreign C call for that whole window,
   which is invisible to Eio's own single-threaded scheduler (releasing the OCaml
   domain lock only unblocks other domains/GC, not Eio's io_uring reactor, since
   that reactor's own progress needs this exact thread back). *)
external consumer_queue_events_enable : kafka_handle -> int -> unit
  = "ocaml_kafka_consumer_queue_events_enable"

external consumer_queue_events_disable : kafka_handle -> unit
  = "ocaml_kafka_consumer_queue_events_disable"

external consumer_queue_poll : kafka_handle -> int -> poll_result
  = "ocaml_rd_kafka_consumer_queue_poll"

external produce_v
  :  kafka_handle -> string -> int32 -> bytes option -> bytes option -> int64
  -> (string * string option) list
  -> (unit, int) result
  = "ocaml_rd_kafka_produce_v_bytecode" "ocaml_rd_kafka_produce_v"

external consumer_close : kafka_handle -> unit
  = "ocaml_rd_kafka_consumer_close"

external assignment_count : kafka_handle -> int
  = "ocaml_rd_kafka_assignment_count"

external assignment : kafka_handle -> (string * int32) list
  = "ocaml_rd_kafka_assignment"

external create_topic_raw : kafka_handle -> string -> int -> int -> int
  = "ocaml_rd_kafka_create_topic"
let create_topic h ~topic_name ~partitions ~replication_factor =
  create_topic_raw h topic_name partitions replication_factor

external commit_message_raw
  :  kafka_handle -> string -> int32 -> int64 -> bool
  -> (unit, int) result
  = "ocaml_rd_kafka_commit_message"
let commit_message h ~topic ~partition ~offset ~async =
  commit_message_raw h topic partition offset async

external commit_all : kafka_handle -> bool -> (unit, int) result
  = "ocaml_rd_kafka_commit_all"

external commit_offsets
  :  kafka_handle -> (string * int32 * int64) list -> bool
  -> (unit, int) result
  = "ocaml_rd_kafka_commit_offsets"

external pipe_create : unit -> int * int
  = "ocaml_kafka_pipe_create"

external delivery_sizeof : unit -> int
  = "ocaml_kafka_delivery_sizeof"
let () = ignore delivery_sizeof   (* suppress warning 32 — used by kafka_producer *)

external read_delivery : int -> int64 * int
  = "ocaml_kafka_read_delivery"

type txn_error = {
  code           : int;
  is_fatal       : bool;
  is_retriable   : bool;
  requires_abort : bool;
}

external init_transactions : kafka_handle -> int -> (unit, txn_error) result
  = "ocaml_rd_kafka_init_transactions"

external begin_transaction : kafka_handle -> (unit, txn_error) result
  = "ocaml_rd_kafka_begin_transaction"

external commit_transaction : kafka_handle -> int -> (unit, txn_error) result
  = "ocaml_rd_kafka_commit_transaction"

external abort_transaction : kafka_handle -> int -> (unit, txn_error) result
  = "ocaml_rd_kafka_abort_transaction"

external send_offsets_to_transaction
  :  kafka_handle -> kafka_handle -> (string * int32 * int64) list -> int
  -> (unit, txn_error) result
  = "ocaml_rd_kafka_send_offsets_to_transaction"

(** Pause / resume delivery for a single partition. Local operations — no
    broker round-trip. Used by consume_partitioned to stop new messages arriving
    for a partition while its retry fiber sleeps. *)
external pause_partition  : kafka_handle -> string -> int32 -> unit = "ocaml_rd_kafka_pause_partition"
external resume_partition : kafka_handle -> string -> int32 -> unit = "ocaml_rd_kafka_resume_partition"
