let contains s sub =
  let slen = String.length s and sublen = String.length sub in
  if sublen = 0 then true
  else if sublen > slen then false
  else
    let rec go i =
      if i > slen - sublen then false
      else if String.sub s i sublen = sub then true
      else go (i + 1)
    in
    go 0

let full_sasl = Kafka_security.{
  mechanism = Plain;
  username  = "user";
  password  = "pass";
}

let test_protocol_of_string_valid () =
  let cases =
    [ ("plaintext", `Plaintext)
    ; ("SSL", `Ssl)
    ; ("sasl_plaintext", `Sasl_plaintext)
    ; ("SASL_SSL", `Sasl_ssl)
    ]
  in
  List.iter (fun (raw, expected) ->
    Alcotest.(check bool) raw true
      (Kafka_security.protocol_of_string raw = Ok expected)
  ) cases

let test_protocol_of_string_invalid () =
  match Kafka_security.protocol_of_string "scram" with
  | Ok _ -> Alcotest.fail "expected invalid protocol error"
  | Error msg ->
    Alcotest.(check bool) "message names env var" true
      (contains msg "KAFKA_SECURITY_PROTOCOL");
    Alcotest.(check bool) "message includes bad value" true
      (contains msg "scram")

let with_env name value f =
  let old = Sys.getenv_opt name in
  Unix.putenv name value;
  Fun.protect f ~finally:(fun () ->
    Unix.putenv name (Option.value old ~default:""))

let test_of_env_rejects_unknown_protocol () =
  with_env "KAFKA_SECURITY_PROTOCOL" "scram" (fun () ->
    match Kafka_security.of_env () with
    | Ok _ -> Alcotest.fail "expected of_env to reject unknown protocol"
    | Error msg ->
      Alcotest.(check bool) "clear protocol error" true
        (contains msg "unknown KAFKA_SECURITY_PROTOCOL"))

let test_of_env_rejects_missing_sasl_fields () =
  with_env "KAFKA_SECURITY_PROTOCOL" "sasl_ssl" (fun () ->
    with_env "KAFKA_SASL_MECHANISM" "" (fun () ->
      with_env "KAFKA_SASL_USERNAME" "user" (fun () ->
        with_env "KAFKA_SASL_PASSWORD" "pass" (fun () ->
          match Kafka_security.of_env () with
          | Ok _ -> Alcotest.fail "expected missing SASL mechanism error"
          | Error msg ->
            Alcotest.(check bool) "message names missing env var" true
              (contains msg "KAFKA_SASL_MECHANISM")))))

let test_of_env_rejects_unknown_sasl_mechanism () =
  with_env "KAFKA_SECURITY_PROTOCOL" "sasl_ssl" (fun () ->
    with_env "KAFKA_SASL_MECHANISM" "GSSAPI" (fun () ->
      with_env "KAFKA_SASL_USERNAME" "user" (fun () ->
        with_env "KAFKA_SASL_PASSWORD" "pass" (fun () ->
          match Kafka_security.of_env () with
          | Ok _ -> Alcotest.fail "expected unknown SASL mechanism error"
          | Error msg ->
            Alcotest.(check bool) "message names bad mechanism" true
              (contains msg "GSSAPI")))))

let test_plaintext_ok () =
  Alcotest.(check bool) "plaintext → Ok" true
    (Kafka_security.settings Kafka_security.default = [ ("security.protocol", "plaintext") ])

let test_ssl_no_sasl_ok () =
  let sec = Kafka_security.Ssl { ssl_ca_location = None } in
  Alcotest.(check bool) "ssl without sasl → Ok" true
    (Kafka_security.settings sec = [ ("security.protocol", "ssl") ])

let test_sasl_ssl_all_fields_ok () =
  let sec = Kafka_security.Sasl_ssl { ssl_ca_location = None; sasl = full_sasl } in
  Alcotest.(check bool) "sasl_ssl all fields → Ok" true
    (Kafka_security.settings sec
     = [ ("security.protocol", "sasl_ssl");
         ("sasl.mechanism", "PLAIN");
         ("sasl.username", "user");
         ("sasl.password", "pass");
       ])

let test_sasl_plaintext_all_fields_ok () =
  let sec = Kafka_security.Sasl_plaintext full_sasl in
  Alcotest.(check bool) "sasl_plaintext all fields → Ok" true
    (Kafka_security.settings sec
     = [ ("security.protocol", "sasl_plaintext");
         ("sasl.mechanism", "PLAIN");
         ("sasl.username", "user");
         ("sasl.password", "pass");
       ])

let () =
  Alcotest.run "kafka_security" [
    "parse", [
      Alcotest.test_case "valid protocols"              `Quick test_protocol_of_string_valid;
      Alcotest.test_case "invalid protocol is an error" `Quick test_protocol_of_string_invalid;
      Alcotest.test_case "of_env rejects unknown protocol" `Quick test_of_env_rejects_unknown_protocol;
      Alcotest.test_case "of_env rejects missing SASL fields" `Quick test_of_env_rejects_missing_sasl_fields;
      Alcotest.test_case "of_env rejects unknown SASL mechanism" `Quick test_of_env_rejects_unknown_sasl_mechanism;
    ];
    "settings", [
      Alcotest.test_case "plaintext"               `Quick test_plaintext_ok;
      Alcotest.test_case "ssl no sasl"             `Quick test_ssl_no_sasl_ok;
      Alcotest.test_case "sasl_ssl all fields"     `Quick test_sasl_ssl_all_fields_ok;
      Alcotest.test_case "sasl_plaintext all fields" `Quick test_sasl_plaintext_all_fields_ok;
    ];
  ]
