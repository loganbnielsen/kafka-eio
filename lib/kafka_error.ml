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

(* rd_kafka_resp_err_t integer mapping.
   Positive = Kafka protocol errors. Negative = librdkafka internal errors. *)
let of_int = function
  (* Kafka protocol errors (positive) *)
  | 0   -> No_error
  | 1   -> Offset_out_of_range
  | 2   -> Invalid_msg
  | 3   -> Unknown_topic_or_part
  | 4   -> Invalid_msg_size
  | 5   -> Leader_not_available
  | 6   -> Not_leader_for_partition
  | 7   -> Request_timed_out
  | 8   -> Broker_not_available
  | 9   -> Replica_not_available
  | 10  -> Msg_size_too_large
  | 11  -> Stale_ctrl_epoch
  | 12  -> Offset_metadata_too_large
  | 13  -> Network_exception
  | 14  -> Coordinator_load_in_progress
  | 15  -> Coordinator_not_available
  | 16  -> Not_coordinator
  | 17  -> Topic_exception
  | 18  -> Record_list_too_large
  | 19  -> Not_enough_replicas
  | 20  -> Not_enough_replicas_after_append
  | 21  -> Invalid_required_acks
  | 22  -> Illegal_generation
  | 23  -> Inconsistent_group_protocol
  | 24  -> Invalid_group_id
  | 25  -> Unknown_member_id
  | 26  -> Invalid_session_timeout
  | 27  -> Rebalance_in_progress
  | 28  -> Invalid_commit_offset_size
  | 29  -> Topic_authorization_failed
  | 30  -> Group_authorization_failed
  | 31  -> Cluster_authorization_failed
  | 32  -> Invalid_timestamp
  | 33  -> Unsupported_sasl_mechanism
  | 34  -> Illegal_sasl_state
  | 35  -> Unsupported_version
  | 36  -> Topic_already_exists
  | 37  -> Invalid_partitions
  | 38  -> Invalid_replication_factor
  | 39  -> Invalid_replica_assignment
  | 40  -> Invalid_config
  | 41  -> Not_controller
  | 42  -> Invalid_request
  | 43  -> Unsupported_for_message_format
  | 44  -> Policy_violation
  | 45  -> Out_of_order_sequence_number
  | 46  -> Duplicate_sequence_number
  | 47  -> Invalid_producer_epoch
  | 48  -> Invalid_txn_state
  | 49  -> Invalid_producer_id_mapping
  | 50  -> Invalid_transaction_timeout
  | 51  -> Concurrent_transactions
  | 52  -> Transaction_coordinator_fenced
  | 53  -> Transactional_id_authorization_failed
  | 54  -> Security_disabled
  | 55  -> Operation_not_attempted
  | 56  -> Kafka_storage_error
  | 57  -> Log_dir_not_found
  | 58  -> Sasl_authentication_failed
  | 59  -> Unknown_producer_id
  | 60  -> Reassignment_in_progress
  | 61  -> Delegation_token_auth_disabled
  | 62  -> Delegation_token_not_found
  | 63  -> Delegation_token_owner_mismatch
  | 64  -> Delegation_token_request_not_allowed
  | 65  -> Delegation_token_authorization_failed
  | 66  -> Delegation_token_expired
  | 67  -> Invalid_principal_type
  | 68  -> Non_empty_group
  | 69  -> Group_id_not_found
  | 70  -> Fetch_session_id_not_found
  | 71  -> Invalid_fetch_session_epoch
  | 72  -> Listener_not_found
  | 73  -> Topic_deletion_disabled
  | 74  -> Fenced_leader_epoch
  | 75  -> Unknown_leader_epoch
  | 76  -> Unsupported_compression_type
  | 77  -> Stale_broker_epoch
  | 78  -> Offset_not_available
  | 79  -> Member_id_required
  | 80  -> Preferred_leader_not_available
  | 81  -> Group_max_size_reached
  | 82  -> Fenced_instance_id
  | 83  -> Eligible_leaders_not_available
  | 84  -> Election_not_needed
  | 85  -> No_reassignment_in_progress
  | 86  -> Group_subscribed_to_topic
  | 87  -> Invalid_record
  | 88  -> Unstable_offset_commit
  | 89  -> Throttling_quota_exceeded
  | 90  -> Producer_fenced
  | 91  -> Resource_not_found
  | 92  -> Duplicate_resource
  | 93  -> Unacceptable_credential
  | 94  -> Inconsistent_voter_set
  | 95  -> Invalid_update_version
  | 96  -> Feature_update_failed
  | 97  -> Principal_deserialization_failure
  (* librdkafka internal errors (negative) *)
  | -1  -> Unknown
  | -200 -> Begin
  | -199 -> Bad_msg
  | -198 -> Bad_compression
  | -197 -> Destroy
  | -196 -> Fail
  | -195 -> Transport
  | -194 -> Crit_sys_resource
  | -193 -> Resolve
  | -192 -> Msg_timed_out
  | -191 -> Partition_eof
  | -190 -> Unknown_partition
  | -189 -> Fs
  | -188 -> Unknown_topic
  | -187 -> All_brokers_down
  | -186 -> Invalid_arg
  | -185 -> Timed_out
  | -184 -> Queue_full
  | -183 -> Isr_insuff
  | -182 -> Node_update
  | -181 -> Ssl
  | -180 -> Wait_coord
  | -179 -> Unknown_group
  | -178 -> In_progress
  | -177 -> Prev_in_progress
  | -176 -> Existing_subscription
  | -175 -> Assign_partitions
  | -174 -> Revoke_partitions
  | -173 -> Conflict
  | -172 -> State
  | -171 -> Unknown_protocol
  | -170 -> Not_implemented
  | -169 -> Authentication
  | -168 -> No_offset
  | -167 -> Outdated
  | -166 -> Timed_out_queue
  | -165 -> Unsupported_feature
  | -164 -> Wait_cache
  | -163 -> Intr
  | -162 -> Key_serialization
  | -161 -> Value_serialization
  | -160 -> Key_deserialization
  | -159 -> Value_deserialization
  | -158 -> Partial
  | -157 -> Read_only
  | -156 -> Noent
  | -155 -> Underflow
  | -154 -> Invalid_type
  | -153 -> Retry
  | -152 -> Purge_queue
  | -151 -> Purge_inflight
  | -150 -> Fatal
  | -149 -> Inconsistent
  | -148 -> Gapless_guarantee
  | -147 -> Max_poll_exceeded
  | -146 -> Unknown_broker
  | -145 -> Not_configured
  | -144 -> Fenced
  | -143 -> Application
  | -142 -> Assignment_lost
  | -141 -> Noop
  | -140 -> Auto_offset_reset
  | -139 -> Log_truncation
  | -100 -> End
  | n    -> Err_unknown n

let to_string = function
  | Config_error msg -> msg
  | err ->
  let code = match err with
    | No_error                        -> 0
    | Offset_out_of_range             -> 1
    | Invalid_msg                     -> 2
    | Unknown_topic_or_part           -> 3
    | Invalid_msg_size                -> 4
    | Leader_not_available            -> 5
    | Not_leader_for_partition        -> 6
    | Request_timed_out               -> 7
    | Broker_not_available            -> 8
    | Replica_not_available           -> 9
    | Msg_size_too_large              -> 10
    | Stale_ctrl_epoch                -> 11
    | Offset_metadata_too_large       -> 12
    | Network_exception               -> 13
    | Coordinator_load_in_progress    -> 14
    | Coordinator_not_available       -> 15
    | Not_coordinator                 -> 16
    | Topic_exception                 -> 17
    | Record_list_too_large           -> 18
    | Not_enough_replicas             -> 19
    | Not_enough_replicas_after_append -> 20
    | Invalid_required_acks           -> 21
    | Illegal_generation              -> 22
    | Inconsistent_group_protocol     -> 23
    | Invalid_group_id                -> 24
    | Unknown_member_id               -> 25
    | Invalid_session_timeout         -> 26
    | Rebalance_in_progress           -> 27
    | Invalid_commit_offset_size      -> 28
    | Topic_authorization_failed      -> 29
    | Group_authorization_failed      -> 30
    | Cluster_authorization_failed    -> 31
    | Invalid_timestamp               -> 32
    | Unsupported_sasl_mechanism      -> 33
    | Illegal_sasl_state              -> 34
    | Unsupported_version             -> 35
    | Topic_already_exists            -> 36
    | Invalid_partitions              -> 37
    | Invalid_replication_factor      -> 38
    | Invalid_replica_assignment      -> 39
    | Invalid_config                  -> 40
    | Not_controller                  -> 41
    | Invalid_request                 -> 42
    | Unsupported_for_message_format  -> 43
    | Policy_violation                -> 44
    | Out_of_order_sequence_number    -> 45
    | Duplicate_sequence_number       -> 46
    | Invalid_producer_epoch          -> 47
    | Invalid_txn_state               -> 48
    | Invalid_producer_id_mapping     -> 49
    | Invalid_transaction_timeout     -> 50
    | Concurrent_transactions         -> 51
    | Transaction_coordinator_fenced  -> 52
    | Transactional_id_authorization_failed -> 53
    | Security_disabled               -> 54
    | Operation_not_attempted         -> 55
    | Kafka_storage_error             -> 56
    | Log_dir_not_found               -> 57
    | Sasl_authentication_failed      -> 58
    | Unknown_producer_id             -> 59
    | Reassignment_in_progress        -> 60
    | Delegation_token_auth_disabled  -> 61
    | Delegation_token_not_found      -> 62
    | Delegation_token_owner_mismatch -> 63
    | Delegation_token_request_not_allowed -> 64
    | Delegation_token_authorization_failed -> 65
    | Delegation_token_expired        -> 66
    | Invalid_principal_type          -> 67
    | Non_empty_group                 -> 68
    | Group_id_not_found              -> 69
    | Fetch_session_id_not_found      -> 70
    | Invalid_fetch_session_epoch     -> 71
    | Listener_not_found              -> 72
    | Topic_deletion_disabled         -> 73
    | Fenced_leader_epoch             -> 74
    | Unknown_leader_epoch            -> 75
    | Unsupported_compression_type    -> 76
    | Stale_broker_epoch              -> 77
    | Offset_not_available            -> 78
    | Member_id_required              -> 79
    | Preferred_leader_not_available  -> 80
    | Group_max_size_reached          -> 81
    | Fenced_instance_id              -> 82
    | Eligible_leaders_not_available  -> 83
    | Election_not_needed             -> 84
    | No_reassignment_in_progress     -> 85
    | Group_subscribed_to_topic       -> 86
    | Invalid_record                  -> 87
    | Unstable_offset_commit          -> 88
    | Throttling_quota_exceeded       -> 89
    | Producer_fenced                 -> 90
    | Resource_not_found              -> 91
    | Duplicate_resource              -> 92
    | Unacceptable_credential         -> 93
    | Inconsistent_voter_set          -> 94
    | Invalid_update_version          -> 95
    | Feature_update_failed           -> 96
    | Principal_deserialization_failure -> 97
    | Unknown                         -> -1
    | Begin                           -> -200
    | Bad_msg                         -> -199
    | Bad_compression                 -> -198
    | Destroy                         -> -197
    | Fail                            -> -196
    | Transport                       -> -195
    | Crit_sys_resource               -> -194
    | Resolve                         -> -193
    | Msg_timed_out                   -> -192
    | Partition_eof                   -> -191
    | Unknown_partition               -> -190
    | Fs                              -> -189
    | Unknown_topic                   -> -188
    | All_brokers_down                -> -187
    | Invalid_arg                     -> -186
    | Timed_out                       -> -185
    | Queue_full                      -> -184
    | Isr_insuff                      -> -183
    | Node_update                     -> -182
    | Ssl                             -> -181
    | Wait_coord                      -> -180
    | Unknown_group                   -> -179
    | In_progress                     -> -178
    | Prev_in_progress                -> -177
    | Existing_subscription           -> -176
    | Assign_partitions               -> -175
    | Revoke_partitions               -> -174
    | Conflict                        -> -173
    | State                           -> -172
    | Unknown_protocol                -> -171
    | Not_implemented                 -> -170
    | Authentication                  -> -169
    | No_offset                       -> -168
    | Outdated                        -> -167
    | Timed_out_queue                 -> -166
    | Unsupported_feature             -> -165
    | Wait_cache                      -> -164
    | Intr                            -> -163
    | Key_serialization               -> -162
    | Value_serialization             -> -161
    | Key_deserialization             -> -160
    | Value_deserialization           -> -159
    | Partial                         -> -158
    | Read_only                       -> -157
    | Noent                           -> -156
    | Underflow                       -> -155
    | Invalid_type                    -> -154
    | Retry                           -> -153
    | Purge_queue                     -> -152
    | Purge_inflight                  -> -151
    | Fatal                           -> -150
    | Inconsistent                    -> -149
    | Gapless_guarantee              -> -148
    | Max_poll_exceeded               -> -147
    | Unknown_broker                  -> -146
    | Not_configured                  -> -145
    | Fenced                          -> -144
    | Application                     -> -143
    | Assignment_lost                 -> -142
    | Noop                            -> -141
    | Auto_offset_reset               -> -140
    | Log_truncation                  -> -139
    | End                             -> -100
    | Err_unknown n                   -> n
    | Config_error _                  -> assert false (* handled above *)
  in
  Kafka_raw.err2str code

let is_retryable = function
  | Leader_not_available
  | Not_leader_for_partition
  | Coordinator_not_available
  | Coordinator_load_in_progress
  | Rebalance_in_progress
  | Request_timed_out
  | Network_exception
  | Transport
  | Timed_out
  | All_brokers_down
  | Resolve -> true
  | _ -> false

let is_fatal = function
  | Fatal
  | Destroy
  | Bad_msg
  | Bad_compression -> true
  | _ -> false
