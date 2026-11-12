#!/bin/sh
set -eu
python3 scripts/create_sample.py
response=$(curl --max-time 30 -fsS -F file=@sample_data/sample.wav http://localhost:8000/jobs)
job_id=$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "Created job $job_id"
deadline=$(($(date +%s) + ${SMOKE_TIMEOUT_SECONDS:-600}))
expected=${EXPECTED_STATUS:-DONE}
while [ "$(date +%s)" -lt "$deadline" ]; do
  result=$(curl --max-time 30 -fsS "http://localhost:8000/jobs/$job_id")
  status=$(printf '%s' "$result" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  echo "Status: $status"
  case "$status" in
    DONE|PARTLY_DONE) [ "$status" = "$expected" ] || exit 1; printf '%s\n' "$result"; exit 0 ;;
    FAILED) printf '%s\n' "$result"; exit 1 ;;
  esac
  sleep 2
done
echo "Timed out waiting for job $job_id" >&2
exit 1
