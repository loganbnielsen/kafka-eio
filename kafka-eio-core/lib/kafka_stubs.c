#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/threads.h>
#include <caml/unixsupport.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <stddef.h>
#include <fcntl.h>
#if defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define htole32(x) OSSwapHostToLittleInt32(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#else
#include <endian.h>
#endif

/* ------------------------------------------------------------------ */
/* Custom block ops and finalizers                                      */
/* ------------------------------------------------------------------ */

/* The GC finalizer is a no-op: Kafka_producer.close() calls ocaml_rd_kafka_destroy
   which nulls this pointer before destroying. Any non-null pointer here means
   close() was not called — we accept the resource leak rather than risk blocking
   the GC (rd_kafka_destroy can block for seconds waiting for broker I/O). */
static void kafka_handle_finalize(value v) {
  *((rd_kafka_t **)Data_custom_val(v)) = NULL;
}

static void kafka_conf_finalize(value v) {
  rd_kafka_conf_t *conf = *((rd_kafka_conf_t **)Data_custom_val(v));
  if (conf) rd_kafka_conf_destroy(conf);
}

static void kafka_topic_finalize(value v) {
  rd_kafka_topic_t *rkt = *((rd_kafka_topic_t **)Data_custom_val(v));
  if (rkt) rd_kafka_topic_destroy(rkt);
}

static struct custom_operations kafka_handle_ops = {
  "kafka_handle", kafka_handle_finalize,
  custom_compare_default, custom_hash_default,
  custom_serialize_default, custom_deserialize_default,
  custom_compare_ext_default, custom_fixed_length_default
};

static struct custom_operations kafka_conf_ops = {
  "kafka_conf", kafka_conf_finalize,
  custom_compare_default, custom_hash_default,
  custom_serialize_default, custom_deserialize_default,
  custom_compare_ext_default, custom_fixed_length_default
};

static struct custom_operations kafka_topic_ops = {
  "kafka_topic", kafka_topic_finalize,
  custom_compare_default, custom_hash_default,
  custom_serialize_default, custom_deserialize_default,
  custom_compare_ext_default, custom_fixed_length_default
};

/* ------------------------------------------------------------------ */
/* Delivery callback — writes a (correlation_id, err_code) pair to a  */
/* pipe. Runs on the librdkafka background thread; no OCaml runtime   */
/* access needed.                                                       */
/* ------------------------------------------------------------------ */

typedef struct {
  int64_t  correlation_id;
  int32_t  err;
} delivery_result_t;

_Static_assert(offsetof(delivery_result_t, correlation_id) == 0,
               "correlation_id offset mismatch — OCaml reads uint64 at byte 0");
_Static_assert(offsetof(delivery_result_t, err) == 8,
               "err offset mismatch — OCaml reads uint32 at byte 8");

static void delivery_cb(rd_kafka_t *rk,
                        const rd_kafka_message_t *msg,
                        void *opaque)
{
  if (!msg->_private) return;   /* no correlation id — fire-and-forget */
  int write_fd = *((int *)opaque);
  delivery_result_t r;
  /* Wire format is fixed little-endian regardless of host byte order —
     OCaml decodes with Cstruct.LE. Without htole*, this struct is written
     in the host's native order, which only happens to agree with LE on
     the little-endian hosts (x86/ARM) this has been run on; a big-endian
     host would decode garbage correlation ids and error codes. */
  r.correlation_id = (int64_t)htole64((uint64_t)(uintptr_t)msg->_private);
  r.err            = (int32_t)htole32((uint32_t)msg->err);
  /* write is async-signal-safe and thread-safe. write_fd is O_NONBLOCK (see
     ocaml_kafka_pipe_create): a full pipe returns EAGAIN immediately instead
     of blocking this librdkafka thread, and we drop the receipt rather than
     retry — the corresponding produce_await promise stays unresolved until
     Kafka_producer.close drains it, rather than stalling delivery entirely. */
  ssize_t n;
  do { n = write(write_fd, &r, sizeof(r)); } while (n < 0 && errno == EINTR);
}

/* ------------------------------------------------------------------ */
/* conf_new                                                             */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_conf_new(value unit) {
  CAMLparam1(unit);
  CAMLlocal1(v);
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  v = caml_alloc_custom(&kafka_conf_ops, sizeof(rd_kafka_conf_t *), 0, 1);
  *((rd_kafka_conf_t **)Data_custom_val(v)) = conf;
  CAMLreturn(v);
}

/* ------------------------------------------------------------------ */
/* conf_set : kafka_conf -> string -> string -> (unit, string) result  */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_conf_set(value conf_v, value key_v, value val_v) {
  CAMLparam3(conf_v, key_v, val_v);
  CAMLlocal2(result, err_str);
  rd_kafka_conf_t *conf = *((rd_kafka_conf_t **)Data_custom_val(conf_v));
  char errstr[512];
  rd_kafka_conf_res_t res = rd_kafka_conf_set(
    conf,
    String_val(key_v),
    String_val(val_v),
    errstr, sizeof(errstr)
  );
  if (res == RD_KAFKA_CONF_OK) {
    result = caml_alloc(1, 0); /* Ok () */
    Store_field(result, 0, Val_unit);
  } else {
    err_str = caml_copy_string(errstr);
    result = caml_alloc(1, 1); /* Error s */
    Store_field(result, 0, err_str);
  }
  CAMLreturn(result);
}

/* ------------------------------------------------------------------ */
/* kafka_new — also installs delivery callback and sets opaque fd      */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_new(value type_v, value conf_v, value write_fd_v) {
  CAMLparam3(type_v, conf_v, write_fd_v);
  CAMLlocal2(result, handle_v);

  /* steal the conf out of the custom block so rd_kafka_new owns it */
  rd_kafka_conf_t *conf = *((rd_kafka_conf_t **)Data_custom_val(conf_v));
  *((rd_kafka_conf_t **)Data_custom_val(conf_v)) = NULL; /* prevent double-free */

  rd_kafka_type_t rk_type = (Int_val(type_v) == 0)
    ? RD_KAFKA_PRODUCER
    : RD_KAFKA_CONSUMER;

  /* Install delivery callback for producers */
  int *fd_ptr = NULL;
  if (rk_type == RD_KAFKA_PRODUCER) {
    fd_ptr = malloc(sizeof(int));
    *fd_ptr = Int_val(write_fd_v);
    rd_kafka_conf_set_dr_msg_cb(conf, delivery_cb);
    rd_kafka_conf_set_opaque(conf, fd_ptr);
  }

  char errstr[512];
  rd_kafka_t *rk = rd_kafka_new(rk_type, conf, errstr, sizeof(errstr));
  if (!rk) {
    free(fd_ptr); /* librdkafka destroyed conf but not our opaque; free(NULL) is a no-op */
    CAMLlocal1(err_str);
    err_str = caml_copy_string(errstr);
    result = caml_alloc(1, 1); /* Error s */
    Store_field(result, 0, err_str);
  } else {
    handle_v = caml_alloc_custom(&kafka_handle_ops, sizeof(rd_kafka_t *), 0, 1);
    *((rd_kafka_t **)Data_custom_val(handle_v)) = rk;
    result = caml_alloc(1, 0); /* Ok handle */
    Store_field(result, 0, handle_v);
  }
  CAMLreturn(result);
}

/* ------------------------------------------------------------------ */
/* subscribe (consumer)                                                 */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_subscribe(value handle_v, value topics_v) {
  CAMLparam2(handle_v, topics_v);
  CAMLlocal2(result, head);

  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(4);

  value lst = topics_v;
  while (lst != Val_emptylist) {
    head = Field(lst, 0);
    rd_kafka_topic_partition_list_add(tpl, String_val(head), RD_KAFKA_PARTITION_UA);
    lst = Field(lst, 1);
  }

  rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, tpl);
  rd_kafka_topic_partition_list_destroy(tpl);

  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    result = caml_alloc(1, 0);
    Store_field(result, 0, Val_unit);
  } else {
    CAMLlocal1(err_v);
    err_v = caml_copy_string(rd_kafka_err2str(err));
    result = caml_alloc(1, 1);
    Store_field(result, 0, err_v);
  }
  CAMLreturn(result);
}

/* ------------------------------------------------------------------ */
/* consumer_poll — returns a message or None                            */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_consumer_poll(value handle_v, value timeout_v) {
  CAMLparam2(handle_v, timeout_v);
  CAMLlocal3(msg_rec, some_v, key_opt);
  CAMLlocal3(hdr_list, hdr_cell, hdr_pair);
  CAMLlocal3(topic_v, part_v, offset_v);
  CAMLlocal3(val_opt, val_bytes, ts_opt);
  CAMLlocal3(hdr_name_v, hdr_val_bytes, hdr_val_opt);

  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int timeout_ms = Int_val(timeout_v);          /* read before releasing lock */
  caml_release_runtime_system();
  rd_kafka_message_t *msg = rd_kafka_consumer_poll(rk, timeout_ms);
  caml_acquire_runtime_system();
  /* all OCaml allocation below — safe after re-acquiring */

  if (!msg)
    CAMLreturn(Val_int(0)); /* Timeout */

  if (msg->err) {
    /* RD_KAFKA_RESP_ERR__PARTITION_EOF is informational; every other
       message-level error (auth failure, unknown topic, max-poll-interval
       exceeded, ...) is surfaced as Poll_error rather than silently folded
       into "no message" — a caller could otherwise spin forever never
       learning the consumer is dead or unauthorized. */
    rd_kafka_resp_err_t poll_err = msg->err;
    rd_kafka_message_destroy(msg);
    if (poll_err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
      CAMLreturn(Val_int(0)); /* Timeout */
    CAMLlocal1(err_block);
    err_block = caml_alloc(1, 1); /* Poll_error tag = 1 */
    Store_field(err_block, 0, Val_int((int)poll_err));
    CAMLreturn(err_block);
  }

  /* Build a 7-tuple: (topic, partition, offset, key_opt, value_opt,
     timestamp_opt, headers with option-valued entries) */

  topic_v  = caml_copy_string(rd_kafka_topic_name(msg->rkt));
  part_v   = caml_copy_int32((int32_t)msg->partition);
  offset_v = caml_copy_int64((int64_t)msg->offset);

  /* librdkafka gives a non-NULL msg->key with key_len == 0 for a genuine
     zero-length key, and NULL only for "no key" — the same distinction
     already made correctly for value/headers below. Gating on key_len > 0
     too (regression note) collapsed a real zero-length key into
     "no key", which Kafka partitions differently (hashed vs. round-robin/
     sticky), silently on the read path only. */
  if (msg->key) {
    CAMLlocal1(key_bytes);
    key_bytes = caml_alloc_string(msg->key_len);
    if (msg->key_len > 0) memcpy(Bytes_val(key_bytes), msg->key, msg->key_len);
    key_opt = caml_alloc(1, 0);   /* Some bytes */
    Store_field(key_opt, 0, key_bytes);
  } else {
    key_opt = Val_int(0);          /* None */
  }

  /* A NULL payload is a Kafka tombstone (delete marker on a compacted
     topic) and must stay distinguishable from a real zero-length value —
     collapsing both to an empty bytes was silent data loss. */
  if (msg->payload) {
    val_bytes = caml_alloc_string(msg->len);
    if (msg->len > 0) memcpy(Bytes_val(val_bytes), msg->payload, msg->len);
    val_opt = caml_alloc(1, 0);   /* Some bytes */
    Store_field(val_opt, 0, val_bytes);
  } else {
    val_opt = Val_int(0);          /* None — tombstone */
  }

  rd_kafka_timestamp_type_t tstype;
  int64_t ts = rd_kafka_message_timestamp(msg, &tstype);
  if (tstype != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE) {
    CAMLlocal1(ts_v);
    ts_v   = caml_copy_int64(ts);
    ts_opt = caml_alloc(1, 0);     /* Some int64 */
    Store_field(ts_opt, 0, ts_v);
  } else {
    ts_opt = Val_int(0);           /* None */
  }

  /* Extract headers before destroy — ownership belongs to msg */
  hdr_list = Val_emptylist;
  rd_kafka_headers_t *hdrs = NULL;
  if (rd_kafka_message_headers(msg, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR && hdrs) {
    size_t hdr_count = rd_kafka_header_cnt(hdrs);
    /* Iterate backwards so the resulting OCaml list preserves header order */
    while (hdr_count > 0) {
      hdr_count--;
      const char *name;
      const void *hval;
      size_t hval_size;
      if (rd_kafka_header_get_all(hdrs, hdr_count, &name, &hval, &hval_size)
          == RD_KAFKA_RESP_ERR_NO_ERROR) {
        hdr_name_v = caml_copy_string(name);
        /* A header can be set with a NULL value distinct from an empty
           one; keep that distinction instead of collapsing both to "". */
        if (hval) {
          hdr_val_bytes = caml_alloc_string(hval_size);
          if (hval_size > 0) memcpy(Bytes_val(hdr_val_bytes), hval, hval_size);
          hdr_val_opt = caml_alloc(1, 0);   /* Some string */
          Store_field(hdr_val_opt, 0, hdr_val_bytes);
        } else {
          hdr_val_opt = Val_int(0);          /* None */
        }
        hdr_pair = caml_alloc_tuple(2);
        Store_field(hdr_pair, 0, hdr_name_v);
        Store_field(hdr_pair, 1, hdr_val_opt);
        hdr_cell = caml_alloc_tuple(2);   /* cons cell */
        Store_field(hdr_cell, 0, hdr_pair);
        Store_field(hdr_cell, 1, hdr_list);
        hdr_list = hdr_cell;
      }
    }
  }

  rd_kafka_message_destroy(msg);

  msg_rec = caml_alloc_tuple(7);
  Store_field(msg_rec, 0, topic_v);
  Store_field(msg_rec, 1, part_v);
  Store_field(msg_rec, 2, offset_v);
  Store_field(msg_rec, 3, key_opt);
  Store_field(msg_rec, 4, val_opt);
  Store_field(msg_rec, 5, ts_opt);
  Store_field(msg_rec, 6, hdr_list);

  /* tag 0 — matches Kafka_raw.poll_result's Msg constructor, the first
     non-constant constructor (Timeout is constant, taking no block tag) */
  some_v = caml_alloc(1, 0);      /* Msg msg_rec */
  Store_field(some_v, 0, msg_rec);
  CAMLreturn(some_v);
}

/* ------------------------------------------------------------------ */
/* topic_new                                                            */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_topic_new(value handle_v, value name_v) {
  CAMLparam2(handle_v, name_v);
  CAMLlocal2(result, topic_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, String_val(name_v), NULL);
  if (!rkt) {
    /* Invalid topic name/config — rd_kafka_topic_new leaves the error on
       rd_kafka_last_error() rather than returning it directly. A null rkt
       stored in the custom block would later crash inside rd_kafka_produce. */
    CAMLlocal1(err_str);
    err_str = caml_copy_string(rd_kafka_err2str(rd_kafka_last_error()));
    result = caml_alloc(1, 1); /* Error s */
    Store_field(result, 0, err_str);
  } else {
    topic_v = caml_alloc_custom(&kafka_topic_ops, sizeof(rd_kafka_topic_t *), 0, 1);
    *((rd_kafka_topic_t **)Data_custom_val(topic_v)) = rkt;
    result = caml_alloc(1, 0); /* Ok topic */
    Store_field(result, 0, topic_v);
  }
  CAMLreturn(result);
}

/* Explicit destroy, mirroring ocaml_rd_kafka_destroy: nulls the pointer
   first so the GC finalizer (kafka_topic_finalize) becomes a no-op,
   making double-destroy impossible. librdkafka's own documented
   lifecycle contract requires every topic object to be destroyed before
   the handle that created it — relying solely on the GC finalizer for
   this (unspecified timing relative to the handle's own destroy) risks
   violating that ordering, so callers that cache kafka_topic values
   (e.g. Kafka_producer's topic_cache) must destroy them explicitly
   before destroying the handle. rd_kafka_topic_destroy is a local
   refcount/state operation, not a network call — no domain-lock
   release needed. */
CAMLprim value ocaml_rd_kafka_topic_destroy(value topic_v) {
  CAMLparam1(topic_v);
  rd_kafka_topic_t *rkt = *((rd_kafka_topic_t **)Data_custom_val(topic_v));
  if (rkt) {
    *((rd_kafka_topic_t **)Data_custom_val(topic_v)) = NULL;
    rd_kafka_topic_destroy(rkt);
  }
  CAMLreturn(Val_unit);
}

/* ------------------------------------------------------------------ */
/* produce : kafka_topic -> partition -> value_opt -> key_opt -> correlation_id -> (unit,int) result */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_produce(value topic_v, value part_v,
                                      value val_v,  value key_opt_v,
                                      value corr_v)
{
  CAMLparam5(topic_v, part_v, val_v, key_opt_v, corr_v);
  CAMLlocal1(result);

  rd_kafka_topic_t *rkt = *((rd_kafka_topic_t **)Data_custom_val(topic_v));
  int32_t partition     = Int32_val(part_v);

  /* None is a tombstone (NULL payload) — rd_kafka_produce with a NULL
     payload/0 length is librdkafka's documented delete-marker convention
     on a compacted topic; distinct from Some Bytes.empty (a real
     zero-length value). */
  void *payload     = NULL;
  size_t payload_sz = 0;
  if (val_v != Val_int(0)) { /* Some bytes */
    value vb = Field(val_v, 0);
    payload    = Bytes_val(vb);
    payload_sz = caml_string_length(vb);
  }

  void *key     = NULL;
  size_t key_sz = 0;
  if (key_opt_v != Val_int(0)) { /* Some bytes */
    value kb = Field(key_opt_v, 0);
    key    = Bytes_val(kb);
    key_sz = caml_string_length(kb);
  }

  /* correlation_id 0 means fire-and-forget; non-zero means awaited */
  void *msg_opaque = (void *)(uintptr_t)(int64_t)Int64_val(corr_v);

  int rc = rd_kafka_produce(
    rkt, partition,
    RD_KAFKA_MSG_F_COPY,
    payload, payload_sz,
    key, key_sz,
    msg_opaque
  );

  if (rc == 0) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    result = caml_alloc(1, 1); Store_field(result, 0, Val_int(errno));
  }
  CAMLreturn(result);
}

/* ------------------------------------------------------------------ */
/* enable_queue_events : kafka_handle -> write_fd -> unit              */
/* Registers write_fd with the librdkafka main queue so that           */
/* one byte (0x01) is written to write_fd whenever the queue           */
/* transitions from empty to non-empty.  The OCaml poll_fiber          */
/* sleeps on the matching read end and calls poll(rk,0) on wake-up.   */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_kafka_enable_queue_events(value handle_v, value write_fd_v) {
  CAMLparam2(handle_v, write_fd_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int write_fd = Int_val(write_fd_v);
  /* rd_kafka_queue_io_event_enable requires a non-blocking fd (librdkafka
     docs). Enforced here, not just by the one caller that happens to set it
     today — this is the generic raw layer, and any future caller passing a
     blocking fd would otherwise hang a librdkafka thread once the pipe fills. */
  if (fcntl(write_fd, F_SETFL, O_NONBLOCK) < 0)
    caml_failwith("kafka_enable_queue_events: fcntl(O_NONBLOCK) failed");
  rd_kafka_queue_t *q = rd_kafka_queue_get_main(rk);
  /* static: librdkafka holds this pointer until deregistered; stack memory
     would become dangling once this function returns. */
  static const char payload = 1;
  rd_kafka_queue_io_event_enable(q, write_fd, &payload, sizeof(payload));
  rd_kafka_queue_destroy(q);
  CAMLreturn(Val_unit);
}

/* ------------------------------------------------------------------ */
/* disable_queue_events : kafka_handle -> unit                         */
/* Clears the io-event callback from the main queue so librdkafka      */
/* stops writing to the pipe write-fd.  Call before closing the pipe   */
/* to prevent stale-fd writes after the fd is recycled by Eio.         */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_kafka_disable_queue_events(value handle_v) {
  CAMLparam1(handle_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  rd_kafka_queue_t *q = rd_kafka_queue_get_main(rk);
  rd_kafka_queue_io_event_enable(q, -1, NULL, 0);
  rd_kafka_queue_destroy(q);
  CAMLreturn(Val_unit);
}

/* ------------------------------------------------------------------ */
/* poll : kafka_handle -> timeout_ms -> int (events served)            */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_poll(value handle_v, value timeout_v) {
  CAMLparam2(handle_v, timeout_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int timeout_ms = Int_val(timeout_v);          /* read before releasing lock */
  caml_release_runtime_system();
  int n = rd_kafka_poll(rk, timeout_ms);
  caml_acquire_runtime_system();
  CAMLreturn(Val_int(n));
}

/* ------------------------------------------------------------------ */
/* err2str                                                              */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_err2str(value code_v) {
  CAMLparam1(code_v);
  CAMLreturn(caml_copy_string(rd_kafka_err2str((rd_kafka_resp_err_t)Int_val(code_v))));
}

/* ------------------------------------------------------------------ */
/* flush                                                                */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_flush(value handle_v, value timeout_v) {
  CAMLparam2(handle_v, timeout_v);
  CAMLlocal1(result);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int timeout_ms = Int_val(timeout_v);   /* read before releasing lock */
  caml_release_runtime_system();
  rd_kafka_resp_err_t err = rd_kafka_flush(rk, timeout_ms);
  caml_acquire_runtime_system();
  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    result = caml_alloc(1, 1); Store_field(result, 0, Val_int((int)err));
  }
  CAMLreturn(result);
}

/* Explicit destroy: nulls the OCaml pointer (preventing finalizer double-destroy)
   and calls rd_kafka_destroy with the domain lock released. */
CAMLprim value ocaml_rd_kafka_destroy(value handle_v) {
  CAMLparam1(handle_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  if (rk) {
    *((rd_kafka_t **)Data_custom_val(handle_v)) = NULL;
    /* Producer handles stash a malloc'd write-fd (see ocaml_rd_kafka_new)
       as the delivery callback's opaque pointer, freed only on a
       kafka_new failure until now — a successful producer's fd_ptr leaked
       on every close. rd_kafka_opaque must be read before destroy, since
       rk is invalid afterward; consumer handles never set one, so this is
       NULL and free is a no-op. */
    void *fd_ptr = rd_kafka_opaque(rk);
    caml_release_runtime_system();
    rd_kafka_destroy(rk);
    caml_acquire_runtime_system();
    free(fd_ptr);
  }
  CAMLreturn(Val_unit);
}

/* ------------------------------------------------------------------ */
/* consumer_close                                                       */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_consumer_close(value handle_v) {
  CAMLparam1(handle_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  if (rk) {
    caml_release_runtime_system();
    rd_kafka_consumer_close(rk);
    caml_acquire_runtime_system();
  }
  CAMLreturn(Val_unit);
}

/* ------------------------------------------------------------------ */
/* assignment_count                                                     */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_assignment_count(value handle_v) {
  CAMLparam1(handle_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  rd_kafka_topic_partition_list_t *parts = NULL;
  int count = 0;
  if (rk && rd_kafka_assignment(rk, &parts) == RD_KAFKA_RESP_ERR_NO_ERROR) {
    if (parts) { count = parts->cnt; rd_kafka_topic_partition_list_destroy(parts); }
  }
  CAMLreturn(Val_int(count));
}

/* ------------------------------------------------------------------ */
/* assignment                                                          */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_assignment(value handle_v) {
  CAMLparam1(handle_v);
  CAMLlocal5(list, cell, pair, topic_v, part_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  rd_kafka_topic_partition_list_t *parts = NULL;

  list = Val_emptylist;
  if (rk && rd_kafka_assignment(rk, &parts) == RD_KAFKA_RESP_ERR_NO_ERROR && parts) {
    for (int i = parts->cnt - 1; i >= 0; i--) {
      topic_v = caml_copy_string(parts->elems[i].topic);
      part_v = caml_copy_int32((int32_t)parts->elems[i].partition);
      pair = caml_alloc(2, 0);
      Store_field(pair, 0, topic_v);
      Store_field(pair, 1, part_v);
      cell = caml_alloc(2, 0);
      Store_field(cell, 0, pair);
      Store_field(cell, 1, list);
      list = cell;
    }
    rd_kafka_topic_partition_list_destroy(parts);
  }
  CAMLreturn(list);
}

/* ------------------------------------------------------------------ */
/* create_topic (librdkafka admin API)                                 */
/* ------------------------------------------------------------------ */

/* Creates a topic using librdkafka's built-in admin API on an existing
   producer handle. Releases the OCaml domain lock while polling for the
   broker response. Returns 0 on success (treating TOPIC_ALREADY_EXISTS as
   success), or a librdkafka error code on failure. */
CAMLprim value ocaml_rd_kafka_create_topic(value handle_v, value topic_v,
                                            value partitions_v, value replicas_v)
{
  CAMLparam4(handle_v, topic_v, partitions_v, replicas_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int partitions = Int_val(partitions_v);
  int replicas   = Int_val(replicas_v);

  /* Copy topic name to C heap before releasing the OCaml runtime. */
  size_t topic_len = caml_string_length(topic_v);
  char *topic_copy = (char *)malloc(topic_len + 1);
  if (!topic_copy) {
    CAMLreturn(Val_int(RD_KAFKA_RESP_ERR__FAIL));
  }
  memcpy(topic_copy, String_val(topic_v), topic_len + 1);

  caml_release_runtime_system();

  char errstr[512];
  rd_kafka_NewTopic_t *new_topic =
    rd_kafka_NewTopic_new(topic_copy, partitions, replicas, errstr, sizeof(errstr));
  free(topic_copy);

  if (!new_topic) {
    caml_acquire_runtime_system();
    CAMLreturn(Val_int(RD_KAFKA_RESP_ERR__INVALID_ARG));
  }

  rd_kafka_AdminOptions_t *options =
    rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
  rd_kafka_queue_t *queue = rd_kafka_queue_new(rk);

  rd_kafka_CreateTopics(rk, &new_topic, 1, options, queue);

  rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 5000 /* ms */);

  int err_code = RD_KAFKA_RESP_ERR__TIMED_OUT;
  if (event) {
    const rd_kafka_CreateTopics_result_t *result =
      rd_kafka_event_CreateTopics_result(event);
    if (result) {
      size_t result_cnt = 0;
      const rd_kafka_topic_result_t **results =
        rd_kafka_CreateTopics_result_topics(result, &result_cnt);
      if (results && result_cnt > 0) {
        err_code = rd_kafka_topic_result_error(results[0]);
        if (err_code == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS)
          err_code = 0;
      } else {
        err_code = 0;
      }
    }
    rd_kafka_event_destroy(event);
  }

  rd_kafka_queue_destroy(queue);
  rd_kafka_AdminOptions_destroy(options);
  rd_kafka_NewTopic_destroy(new_topic);

  caml_acquire_runtime_system();
  CAMLreturn(Val_int(err_code));
}

/* ------------------------------------------------------------------ */
/* commit_message                                                       */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_commit_message(value handle_v, value topic_v,
                                              value part_v,  value offset_v,
                                              value async_v)
{
  CAMLparam5(handle_v, topic_v, part_v, offset_v, async_v);
  CAMLlocal1(result);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int async = Bool_val(async_v); /* read before any release */

  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_add(
    tpl, String_val(topic_v), Int32_val(part_v)
  );
  tp->offset = Int64_val(offset_v) + 1; /* commit next offset */
  caml_release_runtime_system();
  rd_kafka_resp_err_t err = rd_kafka_commit(rk, tpl, async);
  caml_acquire_runtime_system();
  rd_kafka_topic_partition_list_destroy(tpl);

  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    result = caml_alloc(1, 1); Store_field(result, 0, Val_int((int)err));
  }
  CAMLreturn(result);
}

/* commit_offsets: commits an explicit (topic, partition, last-processed-
   offset) list in one call — used by Kafka_consumer.commit_all instead
   of rd_kafka_commit(rk, NULL, ...), which commits each partition's
   current fetch position rather than what was actually processed (see
   Kafka_producer.send_offsets_to_transaction for the same fix applied to
   the transactional path). An empty list commits nothing and succeeds
   trivially, rather than erroring the way a NULL commit does when
   nothing has been fetched/stored yet. */
CAMLprim value ocaml_rd_kafka_commit_offsets(value handle_v, value offsets_v, value async_v)
{
  CAMLparam3(handle_v, offsets_v, async_v);
  CAMLlocal1(result);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int async = Bool_val(async_v);

  int n = 0;
  for (value l = offsets_v; l != Val_emptylist; l = Field(l, 1)) n++;

  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(n);
  for (value l = offsets_v; l != Val_emptylist; l = Field(l, 1)) {
    value tup = Field(l, 0);
    const char *topic = String_val(Field(tup, 0));
    int32_t partition = Int32_val(Field(tup, 1));
    int64_t offset = Int64_val(Field(tup, 2));
    rd_kafka_topic_partition_t *tp =
      rd_kafka_topic_partition_list_add(tpl, topic, partition);
    tp->offset = offset + 1; /* commit next offset, matches commit_message */
  }

  caml_release_runtime_system();
  rd_kafka_resp_err_t err = rd_kafka_commit(rk, tpl, async);
  caml_acquire_runtime_system();
  rd_kafka_topic_partition_list_destroy(tpl);

  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    result = caml_alloc(1, 1); Store_field(result, 0, Val_int((int)err));
  }
  CAMLreturn(result);
}

/* commit_all: rd_kafka_commit(rk, NULL, async) commits the consumer's
   entire current assignment. Split out from commit_message (rather than
   an empty-topic sentinel) so an empty topic name can't be silently
   reinterpreted as "commit everything". */
CAMLprim value ocaml_rd_kafka_commit_all(value handle_v, value async_v)
{
  CAMLparam2(handle_v, async_v);
  CAMLlocal1(result);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int async = Bool_val(async_v);

  caml_release_runtime_system();
  rd_kafka_resp_err_t err = rd_kafka_commit(rk, NULL, async);
  caml_acquire_runtime_system();

  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    result = caml_alloc(1, 1); Store_field(result, 0, Val_int((int)err));
  }
  CAMLreturn(result);
}

/* ------------------------------------------------------------------ */
/* Transactional API                                                    */
/* ------------------------------------------------------------------ */

/* Builds the OCaml Kafka_raw.txn_error record from a non-NULL rd_kafka_error_t
   and destroys it. Must be called with the runtime lock held. is_fatal /
   is_retriable / txn_requires_abort are per-error-instance flags librdkafka
   sets — they are not derivable from the error code alone, so callers need
   them (not just the code) to decide whether to abort, retry, or retire the
   producer. Field order must match Kafka_raw.txn_error's declaration order. */
static value make_txn_error(rd_kafka_error_t *err) {
  CAMLparam0();
  CAMLlocal1(v);
  v = caml_alloc(4, 0);
  Store_field(v, 0, Val_int((int)rd_kafka_error_code(err)));
  Store_field(v, 1, Val_bool(rd_kafka_error_is_fatal(err)));
  Store_field(v, 2, Val_bool(rd_kafka_error_is_retriable(err)));
  Store_field(v, 3, Val_bool(rd_kafka_error_txn_requires_abort(err)));
  rd_kafka_error_destroy(err);
  CAMLreturn(v);
}

CAMLprim value ocaml_rd_kafka_init_transactions(value handle_v, value timeout_v) {
  CAMLparam2(handle_v, timeout_v);
  CAMLlocal2(result, err_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int timeout_ms = Int_val(timeout_v);
  caml_release_runtime_system();
  rd_kafka_error_t *err = rd_kafka_init_transactions(rk, timeout_ms);
  caml_acquire_runtime_system();
  if (!err) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    err_v = make_txn_error(err);
    result = caml_alloc(1, 1); Store_field(result, 0, err_v);
  }
  CAMLreturn(result);
}

CAMLprim value ocaml_rd_kafka_begin_transaction(value handle_v) {
  CAMLparam1(handle_v);
  CAMLlocal2(result, err_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  caml_release_runtime_system();
  rd_kafka_error_t *err = rd_kafka_begin_transaction(rk);
  caml_acquire_runtime_system();
  if (!err) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    err_v = make_txn_error(err);
    result = caml_alloc(1, 1); Store_field(result, 0, err_v);
  }
  CAMLreturn(result);
}

CAMLprim value ocaml_rd_kafka_commit_transaction(value handle_v, value timeout_v) {
  CAMLparam2(handle_v, timeout_v);
  CAMLlocal2(result, err_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int timeout_ms = Int_val(timeout_v);
  caml_release_runtime_system();
  rd_kafka_error_t *err = rd_kafka_commit_transaction(rk, timeout_ms);
  caml_acquire_runtime_system();
  if (!err) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    err_v = make_txn_error(err);
    result = caml_alloc(1, 1); Store_field(result, 0, err_v);
  }
  CAMLreturn(result);
}

CAMLprim value ocaml_rd_kafka_abort_transaction(value handle_v, value timeout_v) {
  CAMLparam2(handle_v, timeout_v);
  CAMLlocal2(result, err_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int timeout_ms = Int_val(timeout_v);
  caml_release_runtime_system();
  rd_kafka_error_t *err = rd_kafka_abort_transaction(rk, timeout_ms);
  caml_acquire_runtime_system();
  if (!err) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    err_v = make_txn_error(err);
    result = caml_alloc(1, 1); Store_field(result, 0, err_v);
  }
  CAMLreturn(result);
}

/* [offsets_v] is an OCaml (string * int32 * int64) list: explicit
   (topic, partition, offset-of-last-processed-message) tuples the caller
   processed inside this transaction. This replaces the prior behavior of
   reading rd_kafka_assignment(cons) directly, which committed the
   consumer's assigned partitions rather than the offsets actually
   processed — a transaction could advance past unprocessed messages if
   the consumer's poll fiber had prefetched ahead. */
CAMLprim value ocaml_rd_kafka_send_offsets_to_transaction(
  value prod_v, value cons_v, value offsets_v, value timeout_v)
{
  CAMLparam4(prod_v, cons_v, offsets_v, timeout_v);
  CAMLlocal2(result, err_v);
  rd_kafka_t *prod = *((rd_kafka_t **)Data_custom_val(prod_v));
  rd_kafka_t *cons = *((rd_kafka_t **)Data_custom_val(cons_v));
  int timeout_ms = Int_val(timeout_v);

  int n = 0;
  for (value l = offsets_v; l != Val_emptylist; l = Field(l, 1)) n++;

  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(n);
  for (value l = offsets_v; l != Val_emptylist; l = Field(l, 1)) {
    value tup = Field(l, 0);
    const char *topic = String_val(Field(tup, 0));
    int32_t partition = Int32_val(Field(tup, 1));
    int64_t offset = Int64_val(Field(tup, 2));
    rd_kafka_topic_partition_t *tp =
      rd_kafka_topic_partition_list_add(tpl, topic, partition);
    tp->offset = offset + 1; /* commit next offset, matches commit_message */
  }

  rd_kafka_consumer_group_metadata_t *cgmd = rd_kafka_consumer_group_metadata(cons);

  caml_release_runtime_system();
  rd_kafka_error_t *err = rd_kafka_send_offsets_to_transaction(
    prod, tpl, cgmd, timeout_ms
  );
  caml_acquire_runtime_system();

  rd_kafka_consumer_group_metadata_destroy(cgmd);
  rd_kafka_topic_partition_list_destroy(tpl);

  if (!err) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    err_v = make_txn_error(err);
    result = caml_alloc(1, 1); Store_field(result, 0, err_v);
  }
  CAMLreturn(result);
}

/* ------------------------------------------------------------------ */
/* delivery_sizeof — returns sizeof(delivery_result_t) so OCaml can   */
/* allocate a Cstruct buffer of exactly the right size.                */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_kafka_delivery_sizeof(value unit) {
  CAMLparam1(unit);
  CAMLreturn(Val_int(sizeof(delivery_result_t)));
}

/* ------------------------------------------------------------------ */
/* pipe_create — creates a non-blocking pipe, returns (read_fd,        */
/* write_fd) as a pair of ints                                          */
/* ------------------------------------------------------------------ */

/* The write end must be non-blocking: delivery_cb below writes one
   delivery_result_t per acknowledged message from a librdkafka background
   thread. If the read side falls behind and the pipe buffer fills, a
   blocking write() would stall that librdkafka thread — and with it,
   delivery, transactions, and shutdown. A full pipe under O_NONBLOCK instead
   returns EAGAIN immediately, and delivery_cb below drops that one receipt
   rather than block. The read end is left blocking; Eio's io_uring-based
   reads do not require O_NONBLOCK. */
CAMLprim value ocaml_kafka_pipe_create(value unit) {
  CAMLparam1(unit);
  CAMLlocal1(pair);
  int fds[2];
  if (pipe(fds) < 0) caml_failwith("kafka_pipe_create: pipe() failed");
  if (fcntl(fds[1], F_SETFL, O_NONBLOCK) < 0) {
    close(fds[0]); close(fds[1]);
    caml_failwith("kafka_pipe_create: fcntl(O_NONBLOCK) failed");
  }
  pair = caml_alloc_tuple(2);
  Store_field(pair, 0, Val_int(fds[0]));
  Store_field(pair, 1, Val_int(fds[1]));
  CAMLreturn(pair);
}

/* ------------------------------------------------------------------ */
/* produce_v — produce with header support via rd_kafka_producev        */
/*                                                                      */
/* Takes kafka_handle (not topic handle) + topic_name string so that   */
/* rd_kafka_producev can be used. Headers are passed as an OCaml list  */
/* of (name, value) string pairs and transferred to librdkafka.        */
/* 7 args → requires both native and bytecode entrypoints.             */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_rd_kafka_produce_v(
  value handle_v, value topic_v, value part_v,
  value val_v,   value key_opt_v, value corr_v, value headers_v)
{
  CAMLparam5(handle_v, topic_v, part_v, val_v, key_opt_v);
  CAMLxparam2(corr_v, headers_v);
  CAMLlocal2(result, hd);

  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  int32_t partition = Int32_val(part_v);

  /* None is a tombstone (NULL payload) — see ocaml_rd_kafka_produce. */
  void *payload     = NULL;
  size_t payload_sz = 0;
  if (val_v != Val_int(0)) { /* Some bytes */
    value vb = Field(val_v, 0);
    payload    = Bytes_val(vb);
    payload_sz = caml_string_length(vb);
  }

  void *key     = NULL;
  size_t key_sz = 0;
  if (key_opt_v != Val_int(0)) {
    value kb = Field(key_opt_v, 0);
    key    = Bytes_val(kb);
    key_sz = caml_string_length(kb);
  }

  void *msg_opaque = (void *)(uintptr_t)(int64_t)Int64_val(corr_v);

  /* Build librdkafka headers from the OCaml (string * string option) list.
     A None value sends a NULL-valued header (rd_kafka_header_add accepts
     a NULL value with size 0), distinct from Some "" — mirroring the
     consumer read side's tombstone-style header handling.
     rd_kafka_producev transfers ownership on success; we destroy on error. */
  rd_kafka_headers_t *hdrs = NULL;
  value lst = headers_v;
  while (lst != Val_emptylist) {
    hd = Field(lst, 0);
    if (!hdrs) hdrs = rd_kafka_headers_new(4);
    const char *hname = String_val(Field(hd, 0));
    value hval_opt     = Field(hd, 1);
    const void *hval   = NULL;
    size_t hval_size   = 0;
    if (hval_opt != Val_int(0)) { /* Some string */
      value hv = Field(hval_opt, 0);
      hval      = String_val(hv);
      hval_size = caml_string_length(hv);
    }
    rd_kafka_header_add(hdrs, hname, -1, hval, hval_size);
    lst = Field(lst, 1);
  }

  rd_kafka_resp_err_t err;
  if (hdrs) {
    err = rd_kafka_producev(rk,
      RD_KAFKA_V_TOPIC(String_val(topic_v)),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
      RD_KAFKA_V_PARTITION(partition),
      RD_KAFKA_V_VALUE(payload, payload_sz),
      RD_KAFKA_V_KEY(key, key_sz),
      RD_KAFKA_V_HEADERS(hdrs),
      RD_KAFKA_V_OPAQUE(msg_opaque),
      RD_KAFKA_V_END);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
      rd_kafka_headers_destroy(hdrs);  /* ownership not transferred on error */
  } else {
    err = rd_kafka_producev(rk,
      RD_KAFKA_V_TOPIC(String_val(topic_v)),
      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
      RD_KAFKA_V_PARTITION(partition),
      RD_KAFKA_V_VALUE(payload, payload_sz),
      RD_KAFKA_V_KEY(key, key_sz),
      RD_KAFKA_V_OPAQUE(msg_opaque),
      RD_KAFKA_V_END);
  }

  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    result = caml_alloc(1, 0); Store_field(result, 0, Val_unit);
  } else {
    result = caml_alloc(1, 1); Store_field(result, 0, Val_int((int)err));
  }
  CAMLreturn(result);
}

/* Bytecode trampoline required for 6+ argument externals */
CAMLprim value ocaml_rd_kafka_produce_v_bytecode(value *argv, int argc) {
  (void)argc;
  return ocaml_rd_kafka_produce_v(
    argv[0], argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);
}

/* ------------------------------------------------------------------ */
/* pipe_read_delivery — reads one delivery_result_t from read_fd.      */
/* Returns (correlation_id, err_code) pair.                             */
/* ------------------------------------------------------------------ */

CAMLprim value ocaml_kafka_read_delivery(value fd_v) {
  CAMLparam1(fd_v);
  CAMLlocal1(pair);
  delivery_result_t r;
  ssize_t n;
  do { n = read(Int_val(fd_v), &r, sizeof(r)); } while (n < 0 && errno == EINTR);
  if (n != sizeof(r)) caml_failwith("kafka_read_delivery: short read");
  pair = caml_alloc_tuple(2);
  Store_field(pair, 0, caml_copy_int64(r.correlation_id));
  Store_field(pair, 1, Val_int((int)r.err));
  CAMLreturn(pair);
}

/* ------------------------------------------------------------------ */
/* pause_partition / resume_partition                                   */
/*                                                                      */
/* Local operations: modify the consumer handle's fetch state for one  */
/* partition without any broker round-trip, so no lock release needed. */
/* Used by consume_partitioned to halt delivery to a partition while    */
/* its fiber sleeps during retry backoff.                               */
/* ------------------------------------------------------------------ */

value ocaml_rd_kafka_pause_partition(value handle_v, value topic_v, value part_v)
{
  CAMLparam3(handle_v, topic_v, part_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  if (!rk) CAMLreturn(Val_unit);
  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(tpl, String_val(topic_v),
                                    (int32_t)Int32_val(part_v));
  rd_kafka_pause_partitions(rk, tpl);
  rd_kafka_topic_partition_list_destroy(tpl);
  CAMLreturn(Val_unit);
}

value ocaml_rd_kafka_resume_partition(value handle_v, value topic_v, value part_v)
{
  CAMLparam3(handle_v, topic_v, part_v);
  rd_kafka_t *rk = *((rd_kafka_t **)Data_custom_val(handle_v));
  if (!rk) CAMLreturn(Val_unit);
  rd_kafka_topic_partition_list_t *tpl = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(tpl, String_val(topic_v),
                                    (int32_t)Int32_val(part_v));
  rd_kafka_resume_partitions(rk, tpl);
  rd_kafka_topic_partition_list_destroy(tpl);
  CAMLreturn(Val_unit);
}
