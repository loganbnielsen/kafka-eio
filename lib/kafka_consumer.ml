type 'e handler_result =
  | Continue
  | Stop
  | Error of 'e

type offset_reset =
  | Earliest
  | Latest

type config = {
  brokers      : string list;
  group_id     : string;
  topics       : string list;
  offset_reset : offset_reset;
  auto_commit  : bool;
  security     : Kafka_security.t;
  properties   : (string * string) list;
}

type message = {
  topic     : string;
  partition : int32;
  offset    : int64;
  key       : bytes option;
  value     : bytes option;  (** [None] is a Kafka tombstone, distinct from [Some Bytes.empty] *)
  timestamp : int64 option;
  headers   : (string * string option) list;  (** [None] value is distinct from [Some ""] *)
}

(* Same fd representation used by kafka_producer.ml's wake pipe. *)
external int_of_fd : Unix.file_descr -> int = "%identity"
external fd_of_int : int -> Unix.file_descr = "%identity"

type t = {
  handle      : Kafka_raw.kafka_handle;
  config      : config;
  stream      : message Eio.Stream.t;
  closed      : bool Atomic.t;
  closed_p    : unit Eio.Promise.t;
  closed_r    : unit Eio.Promise.u;
  (* Eio_unix.pipe ties fd lifetime to the switch; close releases these early. *)
  wake_source : Eio_unix.source_ty Eio.Std.r;
  wake_sink   : Eio_unix.sink_ty Eio.Std.r;
  (* Write end of the wake pipe; close writes here to unblock single_read. *)
  wake_fd     : int Atomic.t;
  poll_exited : unit Eio.Promise.t;
  poll_exit_r : unit Eio.Promise.u;
  (* Last explicitly-committed offset per partition. commit_all (when
     auto_commit = false) commits from here instead of asking librdkafka
     to commit the current fetch position, which may be ahead of what was
     actually processed. *)
  last_processed : (string * int32, int64) Hashtbl.t;
}

let err i = Result.error (Kafka_error.of_int i)

let default_on_warning msg = Printf.eprintf "kafka-eio: %s\n%!" msg

let conf_of_config (cfg : config) : (Kafka_raw.kafka_conf, string) result =
  let ( let* ) = Result.bind in
  let conf = Kafka_raw.conf_new () in
  let set k v =
    Kafka_raw.conf_set conf k v
    |> Result.map_error (fun s -> "kafka conf " ^ k ^ ": " ^ s)
  in
  let* () = set "bootstrap.servers" (String.concat "," cfg.brokers) in
  let* () = set "group.id" cfg.group_id in
  let* () = set "auto.offset.reset"
    (match cfg.offset_reset with Earliest -> "earliest" | Latest -> "latest")
  in
  let* () = set "enable.auto.commit" (if cfg.auto_commit then "true" else "false") in
  (* librdkafka 2.x defaults to cooperative-sticky assignment, which needs a
     rebalance_cb we don't install and would never complete. Pin to eager
     (range,roundrobin) so subscribe() -> poll() -> assignment_count > 0
     works without one. *)
  let* () = set "partition.assignment.strategy" "range,roundrobin" in
  let* () =
    List.fold_left
      (fun acc (k, v) -> let* () = acc in set k v)
      (Ok ()) (Kafka_security.settings cfg.security)
  in
  (* Applied last so callers can override or add any librdkafka key this
     module has no typed field for. *)
  let* () =
    List.fold_left (fun acc (k, v) -> let* () = acc in set k v) (Ok ()) cfg.properties
  in
  Ok conf

let tuple_to_message (topic, partition, offset, key, value, timestamp, headers) =
  { topic; partition; offset; key; value; timestamp; headers }

(* Eio fibers must not sit in blocking C polls. Like the producer, watch a
   librdkafka queue with an fd and drain it with timeout 0 after wakeup. *)
let poll_fiber t sw ~on_ready ~on_poll_error =
  let wake_source = t.wake_source and wake_sink = t.wake_sink in
  let write_fd_int =
    Eio_unix.Fd.use_exn "kafka_consumer_queue_wake_fd"
      (Eio_unix.Resource.fd wake_sink) int_of_fd
  in
  Atomic.set t.wake_fd write_fd_int;
  Kafka_raw.consumer_queue_events_enable t.handle write_fd_int;
  (* Daemon: switch release hooks run close, and close is what stops us. *)
  Eio.Fiber.fork_daemon ~sw (fun () ->
    let wake_buf = Cstruct.create 4096 in
    let notified = ref false in
    let prev_assignment = ref None in
    (* Rebalance transitions also wake the consumer queue, so assignment can
       be tracked per wake instead of by fixed-period polling. *)
    let rec drain () =
      let assignment = Kafka_raw.assignment t.handle |> List.sort compare in
      if Some assignment <> !prev_assignment then begin
        if Option.is_some !prev_assignment then Hashtbl.reset t.last_processed;
        prev_assignment := Some assignment
      end;
      if not !notified && assignment <> [] then begin
        notified := true; on_ready ()
      end;
      match Kafka_raw.consumer_queue_poll t.handle 0 with
      | Kafka_raw.Timeout -> ()
      | Kafka_raw.Msg tup ->
        if not !notified then begin notified := true; on_ready () end;
        Eio.Stream.add t.stream (tuple_to_message tup);
        drain ()
      | Kafka_raw.Poll_error code ->
        (* Persistent poll errors can be immediately ready; yield to avoid
           turning an auth/config failure into a tight scheduler loop. *)
        on_poll_error code;
        Eio.Fiber.yield ();
        drain ()
    in
    let rec loop () =
      if Atomic.get t.closed then ()
      else
        match Eio.Flow.single_read wake_source wake_buf with
        | exception (Eio.Cancel.Cancelled _) -> ()
        | exception End_of_file -> ()
        | _n -> drain (); loop ()
    in
    Fun.protect
      ~finally:(fun () -> Eio.Promise.resolve t.poll_exit_r ())
      (fun () ->
        try drain (); loop (); `Stop_daemon
        with Eio.Cancel.Cancelled _ -> `Stop_daemon))

let close t =
  if Atomic.compare_and_set t.closed false true then
    (* Drain before awaiting poll_exited: the poll fiber may be blocked adding
       to a full stream and needs room to reach its closed check. *)
    Eio.Cancel.protect (fun () ->
      Eio.Promise.resolve t.closed_r ();
      Kafka_raw.consumer_queue_events_disable t.handle;
      (* Wake the daemon once librdkafka no longer owns this fd. *)
      let wfd = Atomic.get t.wake_fd in
      if wfd >= 0 then begin
        let buf = Bytes.make 1 '\x01' in
        (try ignore (Unix.write (fd_of_int wfd) buf 0 1)
         with Unix.Unix_error (Unix.EPIPE, _, _) -> ()
            | Unix.Unix_error _ -> ())
      end;
      let rec drain_until_exited () =
        while not (Eio.Stream.is_empty t.stream) do
          ignore (Eio.Stream.take_nonblocking t.stream)
        done;
        if Eio.Promise.peek t.poll_exited = None then begin
          Eio.Fiber.yield ();
          drain_until_exited ()
        end
      in
      drain_until_exited ();
      Kafka_raw.consumer_close t.handle;
      Kafka_raw.destroy t.handle;
      Eio.Flow.close t.wake_source;
      Eio.Flow.close t.wake_sink)

let default_on_poll_error code =
  Printf.eprintf "kafka-eio: consumer poll error: %s\n%!"
    (Kafka_error.to_string (Kafka_error.of_int code))

let create ?(on_ready = ignore) ?(on_poll_error = default_on_poll_error) (cfg : config) ~sw =
  match conf_of_config cfg with
  | Error msg -> Result.error (Kafka_error.Config_error msg)
  | Ok conf ->
  match Kafka_raw.kafka_new Kafka_raw.Consumer conf (-1) with
  | Error msg -> Result.error (Kafka_error.Config_error msg)
  | Ok rk_handle ->
    (match Kafka_raw.subscribe rk_handle cfg.topics with
     | Error msg ->
       Kafka_raw.destroy rk_handle;
       Result.error (Kafka_error.Config_error msg)
     | Ok () ->
       let (poll_exited, poll_exit_r) = Eio.Promise.create () in
       let (closed_p, closed_r) = Eio.Promise.create () in
       let (wake_source, wake_sink) = Eio_unix.pipe sw in
       let t = {
         handle      = rk_handle;
         config      = cfg;
         stream      = Eio.Stream.create 256;
         closed      = Atomic.make false;
         closed_p;
         closed_r;
         wake_source;
         wake_sink;
         wake_fd     = Atomic.make (-1);
         poll_exited;
         poll_exit_r;
         last_processed = Hashtbl.create 4;
       } in
       poll_fiber t sw ~on_ready ~on_poll_error;
       Eio.Switch.on_release sw (fun () -> close t);
       Result.ok t)

let is_closed t = Atomic.get t.closed

let handle t : Kafka_producer.consumer_handle =
  Kafka_producer.consumer_handle (Kafka_consumer_handle.of_raw t.handle)

(* Every explicit commit path (commit, consume's ack, consume_partitioned's
   ack) goes through here so commit_all can later commit exactly what was
   explicitly committed, per partition — see last_processed's comment. *)
let commit_tracked t ~topic ~partition ~offset =
  match Kafka_raw.commit_message t.handle ~topic ~partition ~offset ~async:false with
  | Ok () ->
    Hashtbl.replace t.last_processed (topic, partition) offset;
    Result.ok ()
  | Error i -> err i

let stream t = t.stream

(* closed is checked once before blocking; a concurrent close while already
   blocked in Eio.Stream.take does not interrupt it. *)
let fetch t =
  if is_closed t then Result.error Kafka_error.Destroy
  else Result.ok (Eio.Stream.take t.stream)

let consume t ?(on_warning = default_on_warning) ~handler () =
  let take_or_closed () =
    if is_closed t then None
    else
      Eio.Fiber.first
        (fun () -> Some (Eio.Stream.take t.stream))
        (fun () -> Eio.Promise.await t.closed_p; None)
  in
  let rec loop () =
    match take_or_closed () with
    | None -> Result.ok ()
    | Some msg ->
      let acked = ref false in
      let ack () =
        acked := true;
        if is_closed t then Result.error Kafka_error.Destroy
        else commit_tracked t ~topic:msg.topic ~partition:msg.partition ~offset:msg.offset
      in
      let result = handler msg ~ack in
      (match result with
       | (Continue | Stop) when not !acked ->
         on_warning
           (Printf.sprintf
              "handler returned without calling ack() — offset not committed \
               (topic=%s partition=%ld offset=%Ld)"
              msg.topic msg.partition msg.offset)
       | _ -> ());
      match result with
      | Continue -> loop ()
      | Stop     -> Result.ok ()
      | Error e  -> Result.error e
  in
  loop ()

(* Only poll_fiber may call Kafka_raw.consumer_poll; reading the stream
   non-blocking here avoids two fibers competing for the same consumer. *)
let poll t =
  if is_closed t then Result.error Kafka_error.Destroy
  else Result.ok (Eio.Stream.take_nonblocking t.stream)

let commit t msg =
  if is_closed t then Result.error Kafka_error.Destroy
  else commit_tracked t ~topic:msg.topic ~partition:msg.partition ~offset:msg.offset

let commit_all t =
  if is_closed t then Result.error Kafka_error.Destroy
  else if t.config.auto_commit then
    (* auto_commit = true: no explicit-commit tracking exists, so this
       just commits the current fetch position, same as periodic
       auto-commit. *)
    (match Kafka_raw.commit_all t.handle false with
     | Ok ()   -> Result.ok ()
     | Error i -> err i)
  else
    (* Kafka_raw.commit_all commits each partition's current fetch
       position, not what was actually processed — commit exactly what
       commit_tracked recorded instead. *)
    let offsets =
      Hashtbl.fold (fun (topic, partition) offset acc -> (topic, partition, offset) :: acc)
        t.last_processed []
    in
    (* rd_kafka_commit with a non-NULL empty list returns NO_OFFSET rather
       than success, so the nothing-to-commit case must be special-cased
       here to stay a no-op. *)
    if offsets = [] then Result.ok ()
    else
      match Kafka_raw.commit_offsets t.handle offsets false with
      | Ok ()   -> Result.ok ()
      | Error i -> err i

(* ── Per-partition fiber consumer with retry + pause/resume ──────────────── *)

type retry_policy = {
  base_delay_s : float;
  max_delay_s  : float;
  max_attempts : int;
}

let default_retry = {
  base_delay_s = 1.0;
  max_delay_s  = 600.0;
  max_attempts = -1;
}

let default_queue_capacity = 16

type 'e consume_error =
  | Handler_error of 'e
  | Invalid_config of string

(* Routes each message to a per-partition fiber so retry backoff on one
   partition never blocks another; the partition is paused at the librdkafka
   level during retry sleep. An inner Eio.Switch.run (not ~sw) owns the
   partition fibers so they're joined before returning even if ~sw is
   cancelled mid-call — see the .mli for why this doesn't weaken
   cancellation.

   Partition streams are bounded (queue_capacity); routing_loop adds to them
   synchronously rather than via a forked fiber, since forking one fiber per
   message was tried and reverted — it trades a bounded stall for unbounded
   fiber growth on a backed-up partition. *)
let consume_partitioned t ~sw:_ ~clock ?(retry = default_retry)
    ?(on_retry = fun ~partition:_ ~attempt:_ ~delay_s:_ -> ())
    ?(on_warning = default_on_warning)
    ?(queue_capacity = default_queue_capacity)
    ~handler () =
  if queue_capacity <= 0 then
    Stdlib.Error (Invalid_config "queue_capacity must be positive")
  else
  let stop    = Atomic.make false in
  let stop_p, stop_r = Eio.Promise.create () in
  let first_err = ref None in
  let streams
    : (int32, (message * (unit -> (unit, Kafka_error.t) result)) option Eio.Stream.t) Hashtbl.t =
    Hashtbl.create 4
  in
  let signal_stop () =
    if Atomic.compare_and_set stop false true then
      Eio.Promise.resolve stop_r ()
  in
  Eio.Switch.run (fun sw ->
    (* Watchdog: closing the consumer directly (instead of cancelling ~sw)
       never resolves stop_p, which would otherwise leave routing_loop's
       Fiber.first blocked forever once poll_fiber stops feeding t.stream.
       Poll is_closed and signal stop ourselves, bounding the hang to one
       poll interval. *)
    Eio.Fiber.fork_daemon ~sw (fun () ->
      let rec watch () =
        if Atomic.get stop then `Stop_daemon
        else if is_closed t then (signal_stop (); `Stop_daemon)
        else (Eio.Time.sleep clock 0.1; watch ())
      in
      watch ());
    let get_or_create_stream partition =
      match Hashtbl.find_opt streams partition with
      | Some s -> s
      | None ->
        let stream = Eio.Stream.create queue_capacity in
        Hashtbl.add streams partition stream;
        Eio.Fiber.fork ~sw (fun () ->
          let rec loop () =
            match Eio.Stream.take stream with
            | None -> ()
            | Some (msg, ack) ->
              if Atomic.get stop || is_closed t then loop ()
              else begin
                let acked = ref false in
                let tracked_ack () = acked := true; ack () in
                let rec attempt n =
                  match handler msg ~ack:tracked_ack with
                  | Continue ->
                    if not !acked then
                      on_warning
                        (Printf.sprintf
                           "handler returned Continue without ack() \
                            (topic=%s partition=%ld offset=%Ld)"
                           msg.topic msg.partition msg.offset);
                    loop ()
                  | Stop ->
                    if not !acked then
                      on_warning
                        (Printf.sprintf
                           "handler returned Stop without ack() \
                            (topic=%s partition=%ld offset=%Ld)"
                           msg.topic msg.partition msg.offset);
                    signal_stop ();
                    (* Must drain, not return: routing_loop's blocking
                       Eio.Stream.add could hang forever on an abandoned
                       full queue otherwise. Discards (unacked, so replayed
                       on restart) until the None sentinel arrives. *)
                    loop ()
                  | Error e ->
                    let exhausted =
                      retry.max_attempts >= 0 && n >= retry.max_attempts
                    in
                    if exhausted then begin
                      on_warning
                        (Printf.sprintf
                           "exhausted %d attempt(s) for topic=%s partition=%ld offset=%Ld"
                           (n + 1) msg.topic msg.partition msg.offset);
                      first_err := Some e;
                      signal_stop ();
                      loop () (* see the Stop case above — must drain, not exit *)
                    end else begin
                      let delay =
                        Float.min
                          (retry.base_delay_s *. (2. ** Float.of_int n))
                          retry.max_delay_s
                      in
                      on_warning
                        (Printf.sprintf
                           "attempt %d failed, retrying in %.0fs (topic=%s partition=%ld offset=%Ld)"
                           (n + 1) delay msg.topic msg.partition msg.offset);
                      on_retry ~partition:msg.partition ~attempt:n ~delay_s:delay;
                      if not (is_closed t) then
                        Kafka_raw.pause_partition t.handle msg.topic msg.partition;
                      let interrupted =
                        Eio.Fiber.first
                          (fun () -> Eio.Time.sleep clock delay; false)
                          (fun () -> Eio.Promise.await stop_p; true)
                      in
                      if not (is_closed t) then
                        Kafka_raw.resume_partition t.handle msg.topic msg.partition;
                      if not interrupted then attempt (n + 1)
                      else loop () (* stop_p fired mid-sleep — see the Stop case above *)
                    end
                in
                attempt 0
              end
          in
          loop ()
        );
        stream
    in
    let rec routing_loop () =
      if Atomic.get stop || is_closed t then ()
      else begin
        let msg_opt =
          match Eio.Stream.take_nonblocking t.stream with
          | Some _ as m -> m
          | None ->
            Eio.Fiber.first
              (fun () -> Some (Eio.Stream.take t.stream))
              (fun () -> Eio.Promise.await stop_p; None)
        in
        match msg_opt with
        | None -> ()
        | Some msg ->
          let ack () =
            if is_closed t then Result.error Kafka_error.Destroy
            else commit_tracked t ~topic:msg.topic ~partition:msg.partition ~offset:msg.offset
          in
          Eio.Stream.add (get_or_create_stream msg.partition) (Some (msg, ack));
          routing_loop ()
      end
    in
    routing_loop ();
    Hashtbl.iter (fun _ s -> Eio.Stream.add s None) streams
  );
  match !first_err with
  | Some e -> Stdlib.Error (Handler_error e)
  | None   -> Stdlib.Ok ()
