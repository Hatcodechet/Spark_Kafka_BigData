#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_FILE="${ROOT_DIR}/performance/results.csv"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-electricity_meters}"
INPUT_FILE="${INPUT_FILE:-data/fake_stream.csv}"
DURATION_SECONDS="${DURATION_SECONDS:-300}"
RATES="${RATES:-50 200 500}"

mkdir -p "$(dirname "${RESULTS_FILE}")"
if [[ ! -f "${RESULTS_FILE}" ]]; then
  echo "timestamp,rate,duration_seconds,producer_exit_code,consumer_lag" > "${RESULTS_FILE}"
fi

cd "${ROOT_DIR}"

for rate in ${RATES}; do
  started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "Running producer at ${rate} records/sec for ${DURATION_SECONDS}s"

  runner=()
  if command -v timeout >/dev/null 2>&1; then
    runner=(timeout "${DURATION_SECONDS}")
    loop_args=(--loop)
  elif command -v gtimeout >/dev/null 2>&1; then
    runner=(gtimeout "${DURATION_SECONDS}")
    loop_args=(--loop)
  else
    loop_args=()
    echo "No timeout/gtimeout command found; this run will replay the input once"
  fi

  set +e
  "${runner[@]}" python -m producer.producer \
    --bootstrap "${BOOTSTRAP}" \
    --topic "${TOPIC}" \
    --input-file "${INPUT_FILE}" \
    --rate "${rate}" \
    "${loop_args[@]}"
  exit_code=$?
  set -e

  lag="unknown"
  if command -v kafka-consumer-groups >/dev/null 2>&1; then
    lag="$(kafka-consumer-groups --bootstrap-server "${BOOTSTRAP}" \
      --describe --group spark-electricity-consumer 2>/dev/null \
      | awk 'NR > 1 && $6 ~ /^[0-9-]+$/ {sum += $6} END {if (NR > 1) print sum; else print "unknown"}')"
  fi

  echo "${started_at},${rate},${DURATION_SECONDS},${exit_code},${lag}" >> "${RESULTS_FILE}"
done

echo "Wrote ${RESULTS_FILE}"
