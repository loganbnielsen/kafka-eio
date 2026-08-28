type delivery_mode =
  | At_least_once
  | At_most_once
  | Exactly_once of { transaction_id : string }

type config = {
  brokers       : string list;
  delivery_mode : delivery_mode;
  linger_ms     : int option;
  security      : Kafka_security.t;
  properties    : (string * string) list;
}

(* Unix.file_descr is an int under the hood on all Unix platforms. *)
external int_of_fd : Unix.file_descr -> int = "%identity"
external fd_of_int : int -> Unix.file_descr = "%identity"

type t = {
  handle       : Kafka_raw.kafka_handle;
  topic_cache  : (string, Kafka_raw.kafka_topic) Hashtbl.t;
  delivery_mode: delivery_mode;
  (* Both ends of both pipes are closed explicitly by close, since
     Eio_unix.pipe ties their fd lifetime to the switch, which may long
     outlive any one producer. *)
  pipe_source  : Eio_unix.source_ty Eio.Std.r;
  delivery_sink: Eio_unix.sink_ty Eio.Std.r;
  wake_source  : Eio_unix.source_ty Eio.Std.r;
  wake_sink    : Eio_unix.sink_ty Eio.Std.r;
  pending      : (int64, (unit, Kafka_error.t) result Eio.Promise.u) Hashtbl.t;
  next_id      : int64 ref;
  mutex        : Mutex.t;
  closed       : bool Atomic.t;
  (* write end of the poll wake pipe; -1 if poll_fiber was not started.
     close t writes one byte here to unblock the daemon's single_read. *)
  wake_fd      : int Atomic.t;
  poll_exited  : unit Eio.Promise.t;
  poll_exit_r  : unit Eio.Promise.u;
  (* delivery_fiber blocks in Eio.Flow.read_exact; a wake byte would corrupt
     the fixed-size-struct framing, so close instead races the read against
     this promise and awaits delivery_exited before touching the pipes. *)
  delivery_stop    : unit Eio.Promise.t;
  delivery_stop_r  : unit Eio.Promise.u;
  delivery_exited  : unit Eio.Promise.t;
  delivery_exit_r  : unit Eio.Promise.u;
}

let err i = Error (Kafka_error.of_int i)

let conf_of_config (cfg : config) : (Kafka_raw.kafka_conf, string) result =
  let ( let* ) = Result.bind in
  let conf = Kafka_raw.conf_new () in
  let set k v =
    Kafka_raw.conf_set conf k v
    |> Result.map_error (fun s -> "kafka conf " ^ k ^ ": " ^ s)
  in
  let* () = set "bootstrap.servers" (String.concat "," cfg.brokers) in
  let* () = match cfg.linger_ms with
    | Some ms -> set "linger.ms" (string_of_int ms)
    | None    -> Ok ()
  in
  let* () = Kafka_security.apply conf cfg.security in
  let* () = match cfg.delivery_mode with
    | At_most_once ->
      set "acks" "0"
    | At_least_once ->
      let* () = set "acks" "all" in
      set "enable.idempotence" "true"
    | Exactly_once { transaction_id } ->
      let* () = set "acks" "all" in
      let* () = set "enable.idempotence" "true" in
      set "transactional.id" transaction_id
  in
  (* Applied last so callers can override or add any librdkafka key this
     module has no typed field for. *)
  let* () =
    List.fold_left (fun acc (k, v) -> let* () = acc in set k v) (Ok ()) cfg.properties
  in
  Ok conf

let is_closed t = Atomic.get t.closed

let get_or_create_topic t name =
  match Hashtbl.find_opt t.topic_cache name with
  | Some rkt -> Ok rkt
  | None ->
    match Kafka_raw.topic_new t.handle name with
    | Ok rkt -> Hashtbl.add t.topic_cache name rkt; Ok rkt
    | Error msg ->
      Printf.eprintf "kafka-eio: topic_new %S failed: %s\n%!" name msg;
      Error Kafka_error.Invalid_arg

(* Reads delivery receipts from the pipe one struct at a time via
   Eio.Flow.read_exact, which suspends on io_uring and resumes when the C
   delivery callback writes one delivery_result_t. buf is reused safely
   since parsing is synchronous with no yields between read and use.

   The blocked read is interrupted by racing against delivery_stop rather
   than a wake byte, since a fixed-size struct read can't distinguish a
   wake byte from real framing. *)
let delivery_fiber t sw =
  let sz  = Kafka_raw.delivery_sizeof () in
  let buf = Cstruct.create sz in
  Eio.Fiber.fork_daemon ~sw (fun () ->
    let rec loop () =
      if Atomic.get t.closed then ()
      else
        match
          Eio.Fiber.first
            (fun () -> `Read (Eio.Flow.read_exact t.pipe_source buf))
            (fun () -> Eio.Promise.await t.delivery_stop; `Stop)
        with
        | `Stop -> ()
        | exception End_of_file -> ()
        | `Read () ->
          let corr_id  = Cstruct.LE.get_uint64 buf 0 in
          let err_code = Int32.to_int (Cstruct.LE.get_uint32 buf 8) in
          let result   = if err_code = 0 then Ok () else err err_code in
          Mutex.lock t.mutex;
          Fun.protect
            ~finally:(fun () -> Mutex.unlock t.mutex)
            (fun () ->
              match Hashtbl.find_opt t.pending corr_id with
              | None -> ()
              | Some resolver ->
                Hashtbl.remove t.pending corr_id;
                Eio.Promise.resolve resolver result);
          loop ()
    in
    (try loop () with Eio.Cancel.Cancelled _ -> ());
    Eio.Promise.resolve t.delivery_exit_r ();
    `Stop_daemon)

(* Sleeps at 0% CPU on wake_source; librdkafka writes one byte to the
   matching write end when its main queue goes empty -> non-empty
   (edge-triggered, via enable_queue_events). On wake we drain with
   poll(rk,0), firing delivery callbacks, then yield so Eio can process
   delivery_fiber's io_uring completions.

   write_fd is O_NONBLOCK so librdkafka's thread never blocks if the pipe
   fills (the drain loop catches up regardless of how many bytes arrived);
   a seed drain runs once before the loop for events that landed before
   enable_queue_events was registered. *)
let poll_fiber t sw =
  let wake_source = t.wake_source and wake_sink = t.wake_sink in
  (* ocaml_kafka_enable_queue_events sets O_NONBLOCK on write_fd itself. *)
  let write_fd_int =
    Eio_unix.Fd.use_exn "kafka_queue_wake_fd"
      (Eio_unix.Resource.fd wake_sink) int_of_fd
  in
  Atomic.set t.wake_fd write_fd_int;
  Kafka_raw.enable_queue_events t.handle write_fd_int;
  Eio.Fiber.fork_daemon ~sw (fun () ->
    let wake_buf = Cstruct.create 4096 in
    let rec drain_kafka () =
      let n = Kafka_raw.poll t.handle 0 in
      if n > 0 then drain_kafka ()
    in
    drain_kafka ();  (* seed: clear pre-registration events *)
    let rec loop () =
      if Atomic.get t.closed then ()
      else
        match Eio.Flow.single_read wake_source wake_buf with
        | exception (Eio.Cancel.Cancelled _) -> ()
        | exception End_of_file -> ()
        | _n ->
          drain_kafka ();
          Eio.Fiber.yield ();
          loop ()
    in
    (try loop () with Eio.Cancel.Cancelled _ -> ());
    Eio.Promise.resolve t.poll_exit_r ();
    `Stop_daemon)

let close t =
  if Atomic.compare_and_set t.closed false true then
    (* flush + destroy must run even if the enclosing fiber is cancelled
       mid-shutdown, or librdkafka's background threads keep sending to fds
       that may get recycled by unrelated Eio resources. *)
    Eio.Cancel.protect (fun () ->
      let wfd = Atomic.get t.wake_fd in
      if wfd >= 0 then begin
        Kafka_raw.disable_queue_events t.handle;
        let buf = Bytes.make 1 '\x01' in
        (try ignore (Unix.write (fd_of_int wfd) buf 0 1)
         with Unix.Unix_error (Unix.EPIPE, _, _) -> ()
            | Unix.Unix_error _ -> ());
        Eio.Promise.await t.poll_exited
      end;
      (* flush's internal polling still fires delivery callbacks, so
         delivery_fiber must stay alive through flush and is stopped only
         after it returns. *)
      ignore (Kafka_raw.flush t.handle 5000);
      Eio.Promise.resolve t.delivery_stop_r ();
      Eio.Promise.await t.delivery_exited;
      (* Resolve any produce_await promises that never got a delivery
         receipt — the pipe can drop one under backpressure, or flush can
         time out — so a waiting fiber doesn't hang after close. *)
      let leftover =
        Mutex.lock t.mutex;
        Fun.protect
          ~finally:(fun () -> Mutex.unlock t.mutex)
          (fun () ->
            let leftover = Hashtbl.fold (fun _ r acc -> r :: acc) t.pending [] in
            Hashtbl.reset t.pending;
            leftover)
      in
      List.iter (fun r -> Eio.Promise.resolve r (Error Kafka_error.Destroy)) leftover;
      (* librdkafka requires every cached topic destroyed before the handle
         that created it; a GC finalizer alone doesn't guarantee that
         ordering. *)
      Hashtbl.iter (fun _ rkt -> Kafka_raw.topic_destroy rkt) t.topic_cache;
      Hashtbl.reset t.topic_cache;
      Kafka_raw.destroy t.handle;
      (* Both daemons confirmed exit above, so no pipe is in use — safe to
         close now rather than leak until sw ends. *)
      Eio.Flow.close t.pipe_source;
      Eio.Flow.close t.delivery_sink;
      Eio.Flow.close t.wake_source;
      Eio.Flow.close t.wake_sink)

let create (cfg : config) ~sw =
  let (pipe_source, delivery_sink) = Eio_unix.pipe sw in
  let (wake_source, wake_sink) = Eio_unix.pipe sw in
  (* No fiber is using the pipes yet, so a failed create can close them
     directly instead of leaking until sw ends. *)
  let close_pipes () =
    Eio.Flow.close pipe_source;
    Eio.Flow.close delivery_sink;
    Eio.Flow.close wake_source;
    Eio.Flow.close wake_sink
  in
  let write_fd_int =
    Eio_unix.Fd.use_exn "kafka_write_fd"
      (Eio_unix.Resource.fd delivery_sink) int_of_fd
  in
  match conf_of_config cfg with
  | Error msg -> close_pipes (); Error (Kafka_error.Config_error msg)
  | Ok conf ->
  match Kafka_raw.kafka_new Kafka_raw.Producer conf write_fd_int with
  | Error msg -> close_pipes (); Error (Kafka_error.Config_error msg)
  | Ok handle ->
    let (poll_exited, poll_exit_r) = Eio.Promise.create () in
    let (delivery_stop, delivery_stop_r) = Eio.Promise.create () in
    let (delivery_exited, delivery_exit_r) = Eio.Promise.create () in
    let t = {
      handle;
      topic_cache  = Hashtbl.create 8;
      delivery_mode = cfg.delivery_mode;
      pipe_source;
      delivery_sink;
      wake_source;
      wake_sink;
      pending      = Hashtbl.create 64;
      next_id      = ref 1L;
      mutex        = Mutex.create ();
      closed       = Atomic.make false;
      wake_fd      = Atomic.make (-1);
      poll_exited;
      poll_exit_r;
      delivery_stop;
      delivery_stop_r;
      delivery_exited;
      delivery_exit_r;
    } in
    let start_fibers () =
      delivery_fiber t sw;
      (* poll_fiber drains librdkafka's main queue so delivery callbacks
         (and produce_await promises) fire continuously, not just when a
         transaction happens to call flush/commit/abort. *)
      poll_fiber t sw;
      Eio.Switch.on_release sw (fun () -> close t);
      Ok t
    in
    (match cfg.delivery_mode with
     | Exactly_once _ ->
       (match Kafka_raw.init_transactions handle 5000 with
        | Error e -> Kafka_raw.destroy handle; close_pipes (); Error (Kafka_error.of_int e.code)
        | Ok () -> start_fibers ())
     | _ -> start_fibers ())

let produce t ~topic ~value ?key ?(headers = []) () =
  if is_closed t then Error Kafka_error.Destroy
  else
    match headers with
    | [] ->
      (match get_or_create_topic t topic with
       | Error e -> Error e
       | Ok rkt ->
         match Kafka_raw.produce rkt Int32.minus_one value key 0L with
         | Ok () -> Ok ()
         | Error i -> err i)
    | _ ->
      (match Kafka_raw.produce_v t.handle topic Int32.minus_one value key 0L headers with
       | Ok () -> Ok ()
       | Error i -> err i)

let produce_await t ~topic ~value ?key ?(headers = []) () =
  let promise, resolver = Eio.Promise.create () in
  if is_closed t then begin
    Eio.Promise.resolve resolver (Error Kafka_error.Destroy);
    promise
  end else begin
    let corr_id =
      Mutex.lock t.mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock t.mutex)
        (fun () ->
          let corr_id = !(t.next_id) in
          t.next_id := Int64.add corr_id 1L;
          Hashtbl.add t.pending corr_id resolver;
          corr_id)
    in
    let send_result : (unit, Kafka_error.t) result = match headers with
      | [] ->
        (match get_or_create_topic t topic with
         | Error e -> Error e
         | Ok rkt ->
           match Kafka_raw.produce rkt Int32.minus_one value key corr_id with
           | Ok () -> Ok ()
           | Error i -> err i)
      | _ ->
        (match Kafka_raw.produce_v t.handle topic Int32.minus_one value key corr_id headers with
         | Ok () -> Ok ()
         | Error i -> err i)
    in
    (match send_result with
     | Error e ->
       Mutex.lock t.mutex;
       Fun.protect
         ~finally:(fun () -> Mutex.unlock t.mutex)
         (fun () -> Hashtbl.remove t.pending corr_id);
       Eio.Promise.resolve resolver (Error e)
     | Ok () -> ());
    promise
  end

let create_topic t ~topic_name ~partitions ~replication_factor =
  if is_closed t then Error Kafka_error.Destroy
  else
    match Kafka_raw.create_topic t.handle ~topic_name ~partitions ~replication_factor with
    | 0 -> Ok ()
    | i -> err i

let flush t ~timeout_ms =
  if is_closed t then Error Kafka_error.Destroy
  else
    match Kafka_raw.flush t.handle timeout_ms with
    | Ok ()   -> Ok ()
    | Error i -> err i

type txn_failure = {
  error          : Kafka_error.t;
  is_fatal       : bool;
  is_retriable   : bool;
  requires_abort : bool;
}

type transaction_error =
  | App_error of Kafka_error.t
  | Txn_failure of txn_failure

let string_of_transaction_error = function
  | App_error e -> Kafka_error.to_string e
  | Txn_failure f -> Kafka_error.to_string f.error

let txn_failure_of_raw (e : Kafka_raw.txn_error) : txn_failure = {
  error          = Kafka_error.of_int e.code;
  is_fatal       = e.is_fatal;
  is_retriable   = e.is_retriable;
  requires_abort = e.requires_abort;
}

(* librdkafka reports whether a transactional-call failure requires
   aborting the transaction as a flag on the error itself (not derivable
   from the error code) — abort only when it says so. *)
let abort_if_required t (e : Kafka_raw.txn_error) =
  if e.requires_abort then ignore (Kafka_raw.abort_transaction t.handle 5000)

let with_transaction t ?consumer_offsets f =
  if is_closed t then Error (App_error Kafka_error.Destroy)
  else
  match t.delivery_mode with
  | Exactly_once _ ->
    (match Kafka_raw.begin_transaction t.handle with
     | Error e -> Error (Txn_failure (txn_failure_of_raw e))
     | Ok () ->
       (* Must catch the exception case too, or f raising leaves the
          transaction open: later transactional calls fail with
          state/concurrent-transaction errors. *)
       match f () with
       | exception exn ->
         ignore (Kafka_raw.abort_transaction t.handle 5000);
         raise exn
       | Error _ as e ->
         ignore (Kafka_raw.abort_transaction t.handle 5000);
         Result.map_error (fun k -> App_error k) e
       | Ok () ->
         let offsets_sent =
           match consumer_offsets with
           | None -> Ok ()
           | Some (ch, offsets) ->
             Kafka_raw.send_offsets_to_transaction
               t.handle (Kafka_consumer_handle.to_raw ch) offsets 5000
         in
         (match offsets_sent with
          | Error e -> abort_if_required t e; Error (Txn_failure (txn_failure_of_raw e))
          | Ok () ->
            match Kafka_raw.commit_transaction t.handle 5000 with
            | Ok () -> Ok ()
            | Error e -> abort_if_required t e; Error (Txn_failure (txn_failure_of_raw e))))
  | _ -> Error (App_error Kafka_error.Not_implemented)
