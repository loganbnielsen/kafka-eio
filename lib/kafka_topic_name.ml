type t = string

let of_string name =
  let len = String.length name in
  if len = 0 then
    Error "topic name must not be empty"
  else if len > 249 then
    Error "topic name must be at most 249 bytes"
  else if name = "." || name = ".." then
    Error "topic name must not be '.' or '..'"
  else
    let is_valid_char = function
      | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '.' | '_' | '-' -> true
      | _ -> false
    in
    let rec loop i =
      if i = len then Ok name
      else if is_valid_char name.[i] then loop (i + 1)
      else
        Error (Printf.sprintf
          "topic name contains invalid character %C at byte %d"
          name.[i] i)
    in
    loop 0

let of_string_exn name =
  match of_string name with
  | Ok t -> t
  | Error e -> invalid_arg (Printf.sprintf "invalid Kafka topic name %S: %s" name e)

let to_string t = t
