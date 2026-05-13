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

  set +e
  python -u -m producer.producer \
    --bootstrap "${BOOTSTRAP}" \
    --topic "${TOPIC}" \
    --input-file "${INPUT_FILE}" \
    --rate "${rate}" \
    --loop &
  producer_pid=$!

  sleep "${DURATION_SECONDS}"
  if kill -0 "${producer_pid}" 2>/dev/null; then
    kill "${producer_pid}" 2>/dev/null
    wait "${producer_pid}" 2>/dev/null
    exit_code=$?
  else
    wait "${producer_pid}"
    exit_code=$?
  fi
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
