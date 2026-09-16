#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available; skip Compose smoke."
  exit 1
fi

API_BASE="${API_BASE:-http://localhost:8080/api}"
POLL_ATTEMPTS="${POLL_ATTEMPTS:-60}"
POLL_SLEEP_SECONDS="${POLL_SLEEP_SECONDS:-2}"

wait_for_health() {
  local label="$1"
  echo "Waiting for health ($label)..."
  for _ in $(seq 1 "$POLL_ATTEMPTS"); do
    if curl -fsS "$API_BASE/actuator/health" >/dev/null 2>&1; then
      curl -fsS "$API_BASE/actuator/health"
      echo
      return 0
    fi
    sleep "$POLL_SLEEP_SECONDS"
  done
  echo "Timed out waiting for health ($label)" >&2
  return 1
}

submit_and_await_completed() {
  local label="$1"
  local response workflow_id status
  response="$(curl -fsS -X POST "$API_BASE/workflows" \
    -H "Content-Type: application/json" \
    -d '{"tasks":[{"id":"task-a","type":"success"},{"id":"task-b","type":"success","dependsOn":["task-a"]}]}')"
  echo "$response"
  workflow_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$response")"
  echo "Submitted workflow $workflow_id ($label)"

  for _ in $(seq 1 "$POLL_ATTEMPTS"); do
    response="$(curl -fsS "$API_BASE/workflows/$workflow_id")"
    status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$response")"
    if [[ "$status" == "COMPLETED" ]]; then
      echo "$response" | python3 -c '
import json, sys
payload = json.load(sys.stdin)
statuses = [t.get("status") for t in payload.get("tasks", [])]
assert payload["status"] == "COMPLETED", payload
assert statuses.count("SUCCEEDED") >= 2, payload
print("Workflow completed with dependent tasks succeeded")
'
      return 0
    fi
    if [[ "$status" == "FAILED" || "$status" == "CANCELLED" ]]; then
      echo "Workflow ended in $status ($label): $response" >&2
      return 1
    fi
    sleep "$POLL_SLEEP_SECONDS"
  done
  echo "Timed out waiting for workflow $workflow_id ($label)" >&2
  return 1
}

check_kafka_metrics_best_effort() {
  curl -fsS "$API_BASE/actuator/metrics/relay.kafka.tasks.consumed" >/dev/null 2>&1 \
    && echo "Metric relay.kafka.tasks.consumed is present" \
    || echo "Metric relay.kafka.tasks.consumed not available yet (best-effort)"
  curl -fsS "$API_BASE/actuator/metrics/relay.kafka.consumer.lag" >/dev/null 2>&1 \
    && echo "Metric relay.kafka.consumer.lag is present" \
    || echo "Metric relay.kafka.consumer.lag not available yet (best-effort)"
}

echo "=== Kafka-on distributed smoke ==="
docker compose --profile distributed down -v --remove-orphans >/dev/null 2>&1 || true
KAFKA_ENABLED=true WORKER_ORCHESTRATION_ENABLED=true docker compose --profile distributed up -d --build
wait_for_health "distributed"
submit_and_await_completed "distributed"
check_kafka_metrics_best_effort
docker compose --profile distributed down -v --remove-orphans

echo "=== Kafka-off app smoke (rollback path) ==="
docker compose --profile app down -v --remove-orphans >/dev/null 2>&1 || true
KAFKA_ENABLED=false WORKER_ORCHESTRATION_ENABLED=true docker compose --profile app up -d --build
wait_for_health "app"
submit_and_await_completed "app"
docker compose --profile app down -v --remove-orphans

echo "Compose smoke paths succeeded."
