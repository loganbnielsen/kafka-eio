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

(* Unix.file_descr is an int under the hood on all Unix platforms — same
   representation kafka_producer.ml relies on for its own wake pipe. *)
external int_of_fd : Unix.file_descr -> int = "%identity"
external fd_of_int : int -> Unix.file_descr = "%identity"

type t = {
  handle      : Kafka_raw.kafka_handle;
  config      : config;
  stream      : message Eio.Stream.t;
  closed      : bool Atomic.t;
  (* Both ends kept so close can explicitly close them — Eio_unix.pipe ties
     their fd lifetime to the *switch*, not to this value's own lifetime
     (same reasoning as kafka_producer.ml's identical pipe fields). *)
  wake_source : Eio_unix.source_ty Eio.Std.r;
  wake_sink   : Eio_unix.sink_ty Eio.Std.r;
  (* write end of the consumer-queue wake pipe; -1 until poll_fiber starts.
     close t writes one byte here to unblock the daemon's single_read once
     consumer_queue_events_disable has stopped librdkafka from writing to it
     on its own — mirrors kafka_producer.ml's wake_fd/close exactly. *)
  wake_fd     : int Atomic.t;
  poll_exited : unit Eio.Promise.t;
  poll_exit_r : unit Eio.Promise.u;
  (* Last explicitly-committed offset per partition, updated by every
     successful commit_message call. commit_all (when auto_commit =
     false) reads this instead of asking librdkafka to commit "the
     current assignment" — rd_kafka_commit(rk, NULL, ...) uses each
     partition's current fetch position, not what was actually
     processed, which is the same bug class as #3/#17 fixed earlier for
     transactions, just reached through commit_all instead. *)
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
  (* librdkafka 2.x changed the default assignment strategy to cooperative-sticky,
     which requires a rebalance_cb to drive the multi-round protocol.  Without one
     the rebalance never completes and the consumer never gets assigned partitions.
     Pinning to eager rebalancing (range,roundrobin) preserves the callback-free
     subscribe() → poll() → assignment_count > 0 invariant our poll_fiber relies on. *)
  let* () = set "partition.assignment.strategy" "range,roundrobin" in
  let* () = Kafka_security.apply conf cfg.security in
  (* Applied after every typed/security default above, so advanced users can
     override or extend with any librdkafka key this module has no typed
     field for (client.id, max.poll.interval.ms, custom SASL mechanisms,
     ...) without waiting on a new config field. *)
  let* () =
    List.fold_left (fun acc (k, v) -> let* () = acc in set k v) (Ok ()) cfg.properties
  in
  Ok conf

let tuple_to_message (topic, partition, offset, key, value, timestamp, headers) =
  { topic; partition; offset; key; value; timestamp; headers }

(* Event-driven, like kafka_producer.ml's poll_fiber: consumer_queue_events_enable
   wakes wake_source whenever the *consumer* queue (not the main queue —
   kafka_producer.ml's enable_queue_events watches the main queue, which never
   carries consumer messages) transitions empty -> non-empty. On wake, drain
   with consumer_queue_poll's timeout_ms=0, which returns essentially
   instantly. No call in this loop ever blocks the calling thread for a real
   duration — unlike the old consumer_poll(handle, 100), whose 100ms blocking
   call sat inside a foreign C call for that whole window on every iteration.
   That window was invisible to Eio's own single-threaded scheduler: releasing
   the OCaml domain lock only unblocks other domains and the GC, not Eio's
   io_uring reactor, which needs this exact OS thread back to service any
   other fiber on it — concretely, this starved a concurrent Cohttp_eio.Server
   HTTP listener on the same domain into never accepting a single connection
   for as long as this consumer was alive, reproduced with a minimal repro of
   just those two pieces (see ticket for the isolation steps). *)
let poll_fiber t sw ~on_ready ~on_poll_error =
  let wake_source = t.wake_source and wake_sink = t.wake_sink in
  let write_fd_int =
    Eio_unix.Fd.use_exn "kafka_consumer_queue_wake_fd"
      (Eio_unix.Resource.fd wake_sink) int_of_fd
  in
  Atomic.set t.wake_fd write_fd_int;
  Kafka_raw.consumer_queue_events_enable t.handle write_fd_int;
  (* Must be a daemon fiber, not a plain Fiber.fork: close/destroy run from
     an Eio.Switch.on_release hook (registered in create below), and
     on_release hooks don't fire until every non-daemon fiber forked into
     sw has already finished. A plain fork here deadlocks the moment a
     consumer is left to close implicitly via its switch scope ending
     (rather than an explicit `close` call) — this fiber waits forever for
     t.closed, which only on_release ever sets, which never runs because
     this fiber hasn't finished. Same bug class already hit and fixed once
     below in consume_partitioned's stop watchdog; applying the same fix
     here at the source. *)
  Eio.Fiber.fork_daemon ~sw (fun () ->
    let wake_buf = Cstruct.create 4096 in
    let notified = ref false in
    let prev_assignment = ref None in
    (* Drains every message currently queued (consumer_queue_poll 0 until
       Timeout), checking assignment on every message the same way the old
       loop checked it every ~100ms — a rebalance that lands with no message
       following it won't be caught here until the next wake, which is the
       one behavioral difference from the old fixed-cadence loop (flagged in
       the ticket for a follow-up rebalance-idle test). *)
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
        (* Surfaced rather than silently dropped — auth failures, unknown
           topics, and max-poll-interval violations used to look identical
           to "no message available", so a service could spin forever
           never learning the consumer was dead or unauthorized. *)
        on_poll_error code;
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
    (* Eio.Cancel.protect ensures consumer_close + destroy always run, even when
       the enclosing fiber is cancelled mid-shutdown. Without this, the librdkafka
       background threads keep heartbeating to the broker, holding the consumer group
       open and blocking any subsequent rebalance indefinitely.

       We drain the stream before awaiting poll_exited to break a potential
       deadlock: if close is called while the poll fiber is blocked in
       Eio.Stream.add (stream full, nobody consuming), the poll fiber can never
       see t.closed = true and exit on its own. Draining creates space, unblocks
       the add, and lets the poll fiber reach its t.closed check. *)
    Eio.Cancel.protect (fun () ->
      Kafka_raw.consumer_queue_events_disable t.handle;
      (* Disabling stops librdkafka from writing any *more* wake bytes, but
         the poll fiber may already be blocked in single_read waiting for one
         (the common case — no messages in flight). Same wake-then-await
         pattern as kafka_producer.ml's close: write one byte ourselves so
         the fiber unblocks, reaches its t.closed check, and exits on its
         own rather than this loop spinning on a read that will never
         complete. *)
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
       let (wake_source, wake_sink) = Eio_unix.pipe sw in
       let t = {
         handle      = rk_handle;
         config      = cfg;
         stream      = Eio.Stream.create 256;
         closed      = Atomic.make false;
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

let handle t = Kafka_consumer_handle.of_raw t.handle

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

(* Same use-after-close guard as every other public op: closed is checked
   once before blocking. A concurrent close while a fetch is already
   blocked in Eio.Stream.take is not cancelled by it — the same limitation
   direct stream/Eio.Stream.take use already has, not a new one. *)
let fetch t =
  if is_closed t then Result.error Kafka_error.Destroy
  else Result.ok (Eio.Stream.take t.stream)

let consume t ?(on_warning = default_on_warning) ~handler () =
  let rec loop () =
    let msg = Eio.Stream.take t.stream in
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

(* poll_fiber is always running and is the only fiber allowed to call
   Kafka_raw.consumer_poll — a direct consumer_poll call here would compete
   with it for the same underlying consumer, delivering messages
   nondeterministically to whichever call happened to run first. Reading
   the stream non-blocking keeps exactly one poll loop per consumer while
   still giving callers who don't want [stream]'s blocking [take] a
   non-blocking check. *)
let poll t =
  if is_closed t then Result.error Kafka_error.Destroy
  else Result.ok (Eio.Stream.take_nonblocking t.stream)

let commit t msg =
  if is_closed t then Result.error Kafka_error.Destroy
  else commit_tracked t ~topic:msg.topic ~partition:msg.partition ~offset:msg.offset

let commit_all t =
  if is_closed t then Result.error Kafka_error.Destroy
  else if t.config.auto_commit then
    (* auto_commit = true: no explicit-commit tracking to fall back on —
       committing each partition's current fetch position is exactly what
       periodic auto-commit already does, and its documented risk
       (offset can advance before processing completes) already covers
       this combination; see commit_all's .mli comment. *)
    (match Kafka_raw.commit_all t.handle false with
     | Ok ()   -> Result.ok ()
     | Error i -> err i)
  else
    (* rd_kafka_commit(rk, NULL, ...) — what Kafka_raw.commit_all does —
       uses each partition's current fetch position, not what was
       actually processed (the same bug class as #3/#17, fixed earlier
       for transactions). Commit exactly what commit_tracked recorded
       instead. *)
    let offsets =
      Hashtbl.fold (fun (topic, partition) offset acc -> (topic, partition, offset) :: acc)
        t.last_processed []
    in
    (* Regression note: rd_kafka_commit with a non-NULL but
       empty list returns RD_KAFKA_RESP_ERR__NO_OFFSET, not success — so
       calling commit_all before anything was ever explicitly committed
       (e.g. as a periodic/shutdown flush hook, its natural use) must be
       special-cased here rather than left to librdkafka, or it errors
       on exactly the "nothing to do" case the doc promises is a no-op. *)
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

(* [consume_partitioned t ~sw ~clock ?retry ?on_retry ?on_warning ?queue_capacity
   ~handler] routes each message to a per-partition fiber so retry backoff on
   one partition never blocks another.  During retry sleep the partition is
   paused at the librdkafka level so no new messages accumulate in the
   partition stream.  An inner Eio.Switch.run — not the passed ~sw — owns
   the partition fibers, so they are provably joined before this function
   returns even if ~sw is cancelled mid-call; see the .mli for why this
   doesn't weaken cancellation (the function still runs synchronously
   within the calling fiber, which is what actually matters for ~sw's
   cancellation to take effect here).

   Each partition stream is bounded (queue_capacity) rather than unbounded, to
   cap per-partition buffering. routing_loop dispatches into a partition's
   stream with a direct, synchronous Eio.Stream.add — deliberately, not via a
   forked fiber. Forking one fiber per message to avoid routing_loop blocking
   on a full partition queue was tried and reverted: it replaces a bounded
   stall with unbounded growth, since every message destined for a backed-up
   partition accumulates as its own blocked fiber (each retaining the full
   message) rather than being capped by queue_capacity. A backed-up partition
   can therefore transiently stall routing to other partitions — bounded by
   queue_capacity and further limited by pause_partition already halting new
   deliveries to that partition during retry sleep, which is the common cause
   of sustained backlog. That's the accepted tradeoff for a hard memory bound. *)
let consume_partitioned t ~sw:_ ~clock ?(retry = default_retry)
    ?(on_retry = fun ~partition:_ ~attempt:_ ~delay_s:_ -> ())
    ?(on_warning = default_on_warning)
    ?(queue_capacity = default_queue_capacity)
    ~handler () =
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
    (* Regression note: calling Kafka_consumer.close directly
       (rather than cancelling ~sw, the documented way to stop this
       function) never resolved stop_p, so routing_loop's Fiber.first
       (racing Stream.take against stop_p) blocked forever once
       poll_fiber stopped feeding t.stream — hanging this call
       permanently. This watchdog polls is_closed t and signals stop
       itself, bounding that hang to one poll interval. *)
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
                    (* Regression note: returning here instead of
                       draining left this fiber's queue abandoned-but-full
                       whenever routing_loop or the final None-sentinel add
                       (below) still had something queued for it — both are
                       plain blocking Eio.Stream.add calls, so either could
                       hang forever with nobody left to ever take from this
                       queue again. loop () re-enters the stop-drain branch
                       above, discarding (without acking, so they're
                       replayed on restart) whatever is still queued until
                       the None sentinel arrives, which is always sent
                       after routing_loop returns — guaranteeing this queue
                       is never left permanently full. *)
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
  | Some e -> Result.error e
  | None   -> Result.ok ()
