#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available; skip Compose smoke."
  exit 1
fi

docker compose --profile app up -d --build

for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8080/api/actuator/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

curl -fsS http://localhost:8080/api/actuator/health
echo
curl -fsS -X POST http://localhost:8080/api/workflows \
  -H "Content-Type: application/json" \
  -d '{"tasks":[{"id":"task-a","type":"success"},{"id":"task-b","type":"success","dependsOn":["task-a"]}]}'
echo
echo "Submitted dependent workflow. Inspect /api/workflows and /api/actuator/health."
