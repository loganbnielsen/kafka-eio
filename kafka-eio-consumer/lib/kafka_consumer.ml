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

type t = {
  handle      : Kafka_raw.kafka_handle;
  config      : config;
  stream      : message Eio.Stream.t;
  closed      : bool Atomic.t;
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

(* consumer_poll releases the OCaml runtime lock at the C level, so calling
   it directly from a fiber is safe — the domain is not held during the
   100ms block, and Cancelled is delivered cleanly when the call returns. *)
let poll_fiber t sw ~on_ready ~on_poll_error =
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
    let notified = ref false in
    let prev_assignment = ref None in
    let rec loop () =
      if Atomic.get t.closed then `Stop_daemon
      else begin
        let assignment = Kafka_raw.assignment t.handle |> List.sort compare in
        if Some assignment <> !prev_assignment then begin
          if Option.is_some !prev_assignment then Hashtbl.reset t.last_processed;
          prev_assignment := Some assignment
        end;
        match Kafka_raw.consumer_poll t.handle 100 with
        | Kafka_raw.Timeout ->
          if not !notified && assignment <> [] then begin
            notified := true; on_ready ()
          end;
          Eio.Fiber.yield (); loop ()
        | Kafka_raw.Msg tup ->
          if not !notified then begin notified := true; on_ready () end;
          Eio.Stream.add t.stream (tuple_to_message tup);
          loop ()
        | Kafka_raw.Poll_error code ->
          (* Surfaced rather than silently dropped — auth failures, unknown
             topics, and max-poll-interval violations used to look identical
             to "no message available", so a service could spin forever
             never learning the consumer was dead or unauthorized. *)
          on_poll_error code;
          Eio.Fiber.yield (); loop ()
      end
    in
    let result = try loop () with Eio.Cancel.Cancelled _ -> `Stop_daemon in
    Eio.Promise.resolve t.poll_exit_r ();
    result)

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
      Kafka_raw.destroy t.handle)

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
       let t = {
         handle      = rk_handle;
         config      = cfg;
         stream      = Eio.Stream.create 256;
         closed      = Atomic.make false;
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
