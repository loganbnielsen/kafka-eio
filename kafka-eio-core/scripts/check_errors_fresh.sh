#!/usr/bin/env bash
# Regression note: the RD_KAFKA_RESP_ERR_* -> Kafka_error.t
# mapping is committed by hand from a one-time run of gen_errors.sh, so it
# can silently drift from whatever librdkafka is actually installed. This
# fails (rather than silently falling back to Err_unknown) when the
# installed header defines a code with no matching variant in
# kafka_error.ml, so drift gets caught at release time instead of at
# runtime. Run before a release, or wire into CI.
# Usage: check_errors_fresh.sh [/path/to/rdkafka.h]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HEADER="${1:-/usr/include/librdkafka/rdkafka.h}"
ERROR_ML="${SCRIPT_DIR}/../lib/kafka_error.ml"

missing=0
while IFS= read -r variant; do
  [ -z "$variant" ] && continue
  # End_all has no fixed enum value in rdkafka.h (it's just "last value + 1",
  # a range-boundary sentinel librdkafka never actually returns as an error),
  # so there is no int to map it to — not a real gap.
  [ "$variant" = "End_all" ] && continue
  if ! grep -qE "^\s*\| ${variant}(\s|\$)" "$ERROR_ML"; then
    echo "missing variant for header code: $variant" >&2
    missing=1
  fi
done < <("${SCRIPT_DIR}/gen_errors.sh" "$HEADER" | grep -E '^\s+\|' | sed -E 's/^\s*\|\s*//')

if [ "$missing" -ne 0 ]; then
  echo "" >&2
  echo "kafka_error.ml is missing variant(s) present in $HEADER." >&2
  echo "Regenerate with gen_errors.sh and add the missing variants by hand" >&2
  echo "(to of_int, to_string's code table, is_retryable/is_fatal as needed)." >&2
  exit 1
fi

echo "kafka_error.ml covers every RD_KAFKA_RESP_ERR_* code in $HEADER."
