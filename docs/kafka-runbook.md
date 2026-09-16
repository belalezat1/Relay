# Kafka operator runbook

## Roles

| Process | `WORKER_ORCHESTRATION_ENABLED` | `KAFKA_ENABLED` | Responsibility |
| --- | --- | --- | --- |
| `relay-api` | `true` (default) | on for distributed, off for app rollback | Accepts REST, orchestrates ready work, publishes task commands. Distributed profile sets `KAFKA_TASK_CONSUMER_ENABLED=false`. |
| `relay-worker` | `false` | `true` | Consumes Kafka task/retry messages only. Scale with `--scale relay-worker=N`. |

## Minimum checks

1. Broker and listeners: `GET /api/actuator/health` should show Kafka runtime up when `KAFKA_ENABLED=true`.
2. Consumer lag: `GET /api/actuator/metrics/relay.kafka.consumer.lag`.
3. Dispatch and failure volume: `relay.kafka.tasks.dispatched`, `consumed`, `failed`, `retried`, `dead_lettered`, `duplicate`, `invalid`, `publish_failed`.
4. Dead-lettered work: `GET /api/dead-letters`.
5. Poison / missing-task messages: `GET /api/dispatch-failures`.

## Distinguishing retry, replay, and manual intervention

| State | How to recognize | Operator action |
| --- | --- | --- |
| Retry | Task is `PENDING` with `nextAttemptAt` in the future | Wait. Backoff expires and work is rediscovered. |
| In-flight / claimed | Task is `RUNNING` with a recent claim/lease | Wait for completion or lease expiry. |
| Dead-letter | Task is `DEAD_LETTERED` and listed under `/dead-letters` | Inspect error, then `POST /api/dead-letters/{id}/replay`. |
| Poison message | Row in `/dispatch-failures` | Inspect payload. Do not assume the workflow was mutated. |
| Duplicate delivery | `relay.kafka.tasks.duplicate` increased; task remains `SUCCEEDED` | None. Safe no-op. |

### Dead-letter replay

```bash
curl -X POST http://localhost:8080/api/dead-letters/<id>/replay
```

Replay mutates Postgres only (not Kafka offsets):

- Task returns to `PENDING` with `attemptCount=0` and cleared claim/lease fields
- Workflow returns from `FAILED` to `PENDING` so the orchestrator can run again
- The DLQ row is deleted
- Side effects re-run when the task executes again

Do not reset consumer offsets to force a side-effect rerun of already-succeeded work.

## Local smoke paths

Distributed (API orchestrates, worker consumes):

```bash
KAFKA_ENABLED=true docker compose --profile distributed up -d --build
# or
./scripts/kafka-smoke.sh
```

Kafka-off rollback (single-node in-process execution):

```bash
KAFKA_ENABLED=false docker compose --profile app up -d --build
```

## Rollback

Set `KAFKA_ENABLED=false`. Listeners do not start, work executes in-process via the orchestrator, and Postgres remains the source of truth. No Kafka offset or topic reset is required.
