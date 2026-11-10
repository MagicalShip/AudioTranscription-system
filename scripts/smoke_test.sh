#!/bin/sh
set -eu
python3 scripts/create_sample.py
response=$(curl -fsS -F file=@sample_data/sample.wav http://localhost:8000/jobs)
job_id=$(printf '%s' "$response" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "Created job $job_id"
while :; do
  result=$(curl -fsS "http://localhost:8000/jobs/$job_id")
  status=$(printf '%s' "$result" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  echo "Status: $status"
  case "$status" in
    DONE|PARTLY_DONE) printf '%s\n' "$result"; exit 0 ;;
    FAILED) printf '%s\n' "$result"; exit 1 ;;
  esac
  sleep 2
done
