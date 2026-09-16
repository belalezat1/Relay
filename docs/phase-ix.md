# Phase IX status

Phase IX is **COMPLETE** for Relay v1.0.

## Delivered

- Flyway V8: claim/lease columns, outbox, projections, idempotency outcomes, DLQ support
- Postgres `FOR UPDATE SKIP LOCKED` claiming via `TaskClaimService`
- Retry exponential backoff + jitter (`relay.retry.*`)
- Role split: `relay.worker.orchestration-enabled` / `WORKER_ORCHESTRATION_ENABLED`
  - API orchestrates and publishes (default `true`)
  - Compose `relay-worker` consumes only (`false`)
- `POST /dead-letters/{id}/replay` resets the task to `PENDING`, reopens a `FAILED` workflow, deletes the DLQ row
- Transactional outbox + Kafka task/retry/lifecycle topics
- Compose profiles: `--profile app` (Postgres + API, Kafka off by default) and `--profile distributed` (Kafka + API + consume-only worker)
- Maven API MockMvc tests + core reliability tests
- GitHub Actions: `mvn test` plus Compose smoke job (`scripts/kafka-smoke.sh`)

## Smoke evidence

Local/CI smoke (when Docker is available):

```bash
./scripts/kafka-smoke.sh
```

That script:

1. Starts `--profile distributed` with `KAFKA_ENABLED=true`, submits a dependent workflow, polls until `COMPLETED`
2. Tears down and starts `--profile app` with `KAFKA_ENABLED=false`, confirms the same path completes in-process

**Local note (2026-09-16):** Docker was unavailable on the close-out workstation, so Compose was not executed here. Durable evidence is the GitHub Actions `compose-smoke` job (`ubuntu-latest` + `scripts/kafka-smoke.sh`) alongside `mvn test` (37 core + 3 API MockMvc tests green locally on JDK 21).

## Operator docs

- [architecture.md](architecture.md)
- [runbook.md](runbook.md)
- [kafka-contract.md](kafka-contract.md)
- [kafka-runbook.md](kafka-runbook.md)
- [benchmarks.md](benchmarks.md)

## Non-goals (still out of scope)

- Visual workflow UI
- Multi-region Kafka / cross-region failover
- Replacing Postgres as workflow authority
- Workflow-level HA lease for the orchestrator (API remains the single scheduler in v1)
- Testcontainers-based IT in `mvn test` (Compose smoke is the distributed path)
