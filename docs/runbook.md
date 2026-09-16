# Relay runbook

## Local bring-up

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
cp .env.example .env
# Postgres + Kafka + API (Kafka is required)
docker compose --profile app up -d --build
# Add a second worker process
docker compose --profile distributed up -d --build
```

Health: `GET /api/actuator/health`  
Metrics: `GET /api/actuator/metrics` (includes `relay.kafka.*` and `relay.workflow.events.consumed`)

## Dead letters

```bash
curl http://localhost:8080/api/dead-letters
curl -X POST http://localhost:8080/api/dead-letters/<id>/replay
```

Replay sets the task to PENDING, clears claim/lease fields, marks the DLQ row `replayed_at`, reopens a FAILED workflow to PENDING, and republishes via the outbox.

## Retries

Defaults (Phase IX):

- `relay.retry.backoff-enabled=true` (alias: `relay.kafka.retry-backoff-enabled`)
- Exponential delay from `relay.retry.initial-backoff-seconds` × multiplier^(n)
- Jitter fraction `relay.retry.jitter-fraction` (default 0.2)
- Max attempts `relay.retry.max-attempts` (default 3)

## Outbox stuck / Kafka down

1. Confirm rows in `outbox_events` with `status='PENDING'` (and `next_attempt_at` due).
2. Check API logs for `OutboxPublisher` errors; publish failures stay `PENDING` with backoff until `relay.outbox.max-attempts`, then `FAILED`.
3. Fix broker connectivity — pending rows drain automatically; exhausted `FAILED` rows need operator reset only after max attempts.
4. Inspect `GET /api/dispatch-failures` for poison task payloads.

## Lease recovery

`TaskLeaseRecoveryService` runs on `relay.task.lease-recovery-delay` (default 15s). Crashed workers holding RUNNING leases lose them after `relay.task.claim-lease-seconds`.

## Common failures

| Symptom | Check |
| --- | --- |
| Flyway checksum / validate errors | Ensure V8 applied; do not edit applied migrations |
| Double execution reports | Confirm claim metrics / attempt_count; run concurrent claim tests |
| Duplicate idempotency 400/500 | Another non-terminal task owns the key |
| Listeners not starting | `KAFKA_ENABLED=true` and `spring.kafka.listener.auto-startup` |

## Related docs

- [architecture.md](architecture.md)
- [benchmarks.md](benchmarks.md)
- [kafka-contract.md](kafka-contract.md)
- [kafka-runbook.md](kafka-runbook.md)
