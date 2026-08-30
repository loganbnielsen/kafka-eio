(** Canonical public API for kafka-eio. *)

module Error : sig
  type t =
    | Unknown
    | Offset_out_of_range
    | Invalid_msg
    | Unknown_topic_or_part
    | Invalid_msg_size
    | Leader_not_available
    | Not_leader_for_partition
    | Request_timed_out
    | Broker_not_available
    | Replica_not_available
    | Msg_size_too_large
    | Stale_ctrl_epoch
    | Offset_metadata_too_large
    | Network_exception
    | Coordinator_load_in_progress
    | Coordinator_not_available
    | Not_coordinator
    | Topic_exception
    | Record_list_too_large
    | Not_enough_replicas
    | Not_enough_replicas_after_append
    | Invalid_required_acks
    | Illegal_generation
    | Inconsistent_group_protocol
    | Invalid_group_id
    | Unknown_member_id
    | Invalid_session_timeout
    | Rebalance_in_progress
    | Invalid_commit_offset_size
    | Topic_authorization_failed
    | Group_authorization_failed
    | Cluster_authorization_failed
    | Invalid_timestamp
    | Unsupported_sasl_mechanism
    | Illegal_sasl_state
    | Unsupported_version
    | Topic_already_exists
    | Invalid_partitions
    | Invalid_replication_factor
    | Invalid_replica_assignment
    | Invalid_config
    | Not_controller
    | Invalid_request
    | Unsupported_for_message_format
    | Policy_violation
    | Out_of_order_sequence_number
    | Duplicate_sequence_number
    | Invalid_producer_epoch
    | Invalid_txn_state
    | Invalid_producer_id_mapping
    | Invalid_transaction_timeout
    | Concurrent_transactions
    | Transaction_coordinator_fenced
    | Transactional_id_authorization_failed
    | Security_disabled
    | Operation_not_attempted
    | Kafka_storage_error
    | Log_dir_not_found
    | Sasl_authentication_failed
    | Unknown_producer_id
    | Reassignment_in_progress
    | Delegation_token_auth_disabled
    | Delegation_token_not_found
    | Delegation_token_owner_mismatch
    | Delegation_token_request_not_allowed
    | Delegation_token_authorization_failed
    | Delegation_token_expired
    | Invalid_principal_type
    | Non_empty_group
    | Group_id_not_found
    | Fetch_session_id_not_found
    | Invalid_fetch_session_epoch
    | Listener_not_found
    | Topic_deletion_disabled
    | Fenced_leader_epoch
    | Unknown_leader_epoch
    | Unsupported_compression_type
    | Stale_broker_epoch
    | Offset_not_available
    | Member_id_required
    | Preferred_leader_not_available
    | Group_max_size_reached
    | Fenced_instance_id
    | Eligible_leaders_not_available
    | Election_not_needed
    | No_reassignment_in_progress
    | Group_subscribed_to_topic
    | Invalid_record
    | Unstable_offset_commit
    | Throttling_quota_exceeded
    | Producer_fenced
    | Resource_not_found
    | Duplicate_resource
    | Unacceptable_credential
    | Inconsistent_voter_set
    | Invalid_update_version
    | Feature_update_failed
    | Principal_deserialization_failure
    | No_error
    | Begin
    | Bad_msg
    | Bad_compression
    | Destroy
    | Fail
    | Transport
    | Crit_sys_resource
    | Resolve
    | Msg_timed_out
    | Partition_eof
    | Unknown_partition
    | Fs
    | Unknown_topic
    | All_brokers_down
    | Invalid_arg
    | Timed_out
    | Queue_full
    | Isr_insuff
    | Node_update
    | Ssl
    | Wait_coord
    | Unknown_group
    | In_progress
    | Prev_in_progress
    | Existing_subscription
    | Assign_partitions
    | Revoke_partitions
    | Conflict
    | State
    | Unknown_protocol
    | Not_implemented
    | Authentication
    | No_offset
    | Outdated
    | Timed_out_queue
    | Unsupported_feature
    | Wait_cache
    | Intr
    | Key_serialization
    | Value_serialization
    | Key_deserialization
    | Value_deserialization
    | Partial
    | Read_only
    | Noent
    | Underflow
    | Invalid_type
    | Retry
    | Purge_queue
    | Purge_inflight
    | Fatal
    | Inconsistent
    | Gapless_guarantee
    | Max_poll_exceeded
    | Unknown_broker
    | Not_configured
    | Fenced
    | Application
    | Assignment_lost
    | Noop
    | Auto_offset_reset
    | Log_truncation
    | End
    | Err_unknown of int
    | Config_error of string

  val of_int : int -> t
  val to_string : t -> string
  val is_retryable : t -> bool
  val is_fatal : t -> bool
end

module Security : sig
  type protocol =
    [ `Plaintext
    | `Ssl
    | `Sasl_plaintext
    | `Sasl_ssl
    ]

  type sasl_mechanism =
    | Plain
    | Scram_sha256
    | Scram_sha512

  type sasl = {
    mechanism : sasl_mechanism;
    username  : string;
    password  : string;
  }

  type t =
    | Plaintext
    | Ssl of { ssl_ca_location : string option }
    | Sasl_plaintext of sasl
    | Sasl_ssl of { ssl_ca_location : string option; sasl : sasl }

  val default : t
  val protocol_of_string : string -> (protocol, string) result
  val of_env : unit -> (t, string) result
  val settings : t -> (string * string) list
end

module Producer : sig
  type delivery_mode =
    | At_least_once
    | At_most_once
    | Exactly_once of { transaction_id : string }

  type config = {
    brokers       : string list;
    delivery_mode : delivery_mode;
    linger_ms     : int option;
    security      : Security.t;
    properties    : (string * string) list;
  }

  type t

  val create : config -> sw:Eio.Switch.t -> (t, Error.t) result
  val close : t -> unit

  val create_topic
    :  t
    -> topic_name:string
    -> partitions:int
    -> replication_factor:int
    -> (unit, Error.t) result

  val produce
    :  t
    -> topic:string
    -> value:bytes option
    -> ?key:bytes
    -> ?headers:(string * string option) list
    -> unit
    -> (unit, Error.t) result

  val produce_await
    :  t
    -> topic:string
    -> value:bytes option
    -> ?key:bytes
    -> ?headers:(string * string option) list
    -> unit
    -> (unit, Error.t) result Eio.Promise.t

  val flush : t -> timeout_ms:int -> (unit, Error.t) result

  type txn_failure = {
    error          : Error.t;
    is_fatal       : bool;
    is_retriable   : bool;
    requires_abort : bool;
  }

  type consumer_handle

  type transaction_error =
    | App_error of Error.t
    | Txn_failure of txn_failure

  val string_of_transaction_error : transaction_error -> string

  val with_transaction
    :  t
    -> ?consumer_offsets:(consumer_handle * (string * int32 * int64) list)
    -> (unit -> (unit, Error.t) result)
    -> (unit, transaction_error) result
end

module Consumer : sig
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
    security     : Security.t;
    properties   : (string * string) list;
  }

  type message = {
    topic     : string;
    partition : int32;
    offset    : int64;
    key       : bytes option;
    value     : bytes option;
    timestamp : int64 option;
    headers   : (string * string option) list;
  }

  type t

  val create
    :  ?on_ready:(unit -> unit)
    -> ?on_poll_error:(int -> unit)
    -> config
    -> sw:Eio.Switch.t
    -> (t, Error.t) result

  val close : t -> unit
  val handle : t -> Producer.consumer_handle
  val fetch : t -> (message, Error.t) result
  val poll : t -> (message option, Error.t) result
  val commit : t -> message -> (unit, Error.t) result
  val commit_all : t -> (unit, Error.t) result

  (** Process messages until the handler returns [Stop], the consumer is
      closed, or the handler returns [Error _]. Closing the consumer stops the
      loop as [Ok ()]; handler errors are returned unchanged. *)
  val consume
    :  t
    -> ?on_warning:(string -> unit)
    -> handler:(message -> ack:(unit -> (unit, Error.t) result) -> 'e handler_result)
    -> unit
    -> (unit, 'e) result

  type retry_policy = {
    base_delay_s : float;
    max_delay_s  : float;
    max_attempts : int;
  }

  val default_retry : retry_policy
  val default_queue_capacity : int

  type 'e consume_error =
    | Handler_errors of (int32 * 'e) list
    | Invalid_config of string

  val consume_partitioned
    :  t
    -> sw:Eio.Switch.t
    -> clock:_ Eio.Time.clock
    -> ?retry:retry_policy
    -> ?on_retry:(partition:int32 -> attempt:int -> delay_s:float -> unit)
    -> ?on_warning:(string -> unit)
    -> ?queue_capacity:int
    -> handler:(message -> ack:(unit -> (unit, Error.t) result) -> 'e handler_result)
    -> unit
    -> (unit, 'e consume_error) result
end
