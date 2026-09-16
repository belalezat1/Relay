# Kafka operations contract

Postgres remains the authoritative workflow and task store. Kafka transports task commands and workflow lifecycle events.

## Topics

| Topic | Key | Purpose | Ordering |
| --- | --- | --- | --- |
| `relay.workflow.tasks` | workflow id | Task dispatch commands for Kafka-backed workers | Per-workflow: tasks for one workflow land on one partition |
| `relay.workflow.tasks.retry` | workflow id | Delayed retry commands after task failure | Delay enforced via outbox `next_attempt_at` before publish |
| `relay.workflow.events` | workflow id | Workflow lifecycle audit events | Per-workflow event order only |

Topics are auto-created in local Compose. Production should create them explicitly with enough partitions for worker concurrency. Partition count does not change dependency ordering; Postgres still decides which tasks are ready.

## Consumer groups

| Group | Listener | Notes |
| --- | --- | --- |
| `relay-workflow-task-group` | `TaskDispatchConsumer` | Competing consumers execute dispatched tasks. Scale workers by adding group members. |
| `relay-workflow-task-retry-group` | `TaskRetryConsumer` | Promotes due retries onto the main task topic via the outbox. |
| `relay-workflow-events` | `WorkflowKafkaConsumer` | Observes lifecycle events. Does not mutate workflow state. |

## Retry and dead-letter behavior

Kafka is required for Relay (`KAFKA_ENABLED` defaults to `true`).

1. A retryable task failure writes `PENDING` plus `next_attempt_at` in Postgres and enqueues an outbox row for `relay.workflow.tasks.retry`.
2. After the delay, `OutboxPublisher` publishes to the retry topic; `TaskRetryConsumer` promotes the message onto the main task dispatch topic.
3. Terminal failures write `DEAD_LETTERED` in Postgres and a row in `dead_letter_tasks`.
4. Malformed or missing-task messages are acknowledged after they are stored in `kafka_dispatch_failures` so they cannot poison the consumer.

Replay is operator-driven: inspect `/api/dead-letters` or `/api/dispatch-failures`, then resubmit the workflow. Do not republish a succeeded task to force a side-effect rerun; duplicate deliveries of succeeded work are no-ops.

## Idempotency and crash recovery

- Execution claims are stored on `tasks.execution_claimed_at` with a configurable lease (`relay.task.claim-lease-seconds`, default 300s).
- Two consumers cannot both begin the same task while the lease is active.
- Duplicate deliveries of `SUCCEEDED` or `DEAD_LETTERED` tasks are ignored and counted as `relay.kafka.tasks.duplicate`.
- If a worker crashes after claiming and before completion, `TaskLeaseRecoveryService` returns the task to `PENDING` after the lease expires so the orchestrator can rediscover and republish it.
- Stale `QUEUED` tasks older than the lease are returned to `PENDING` so a failed publish cannot strand work.

## Configuration names

Keep these aligned across `application.properties`, `application-prod.properties`, Compose, and `.env`:

- `KAFKA_ENABLED` / `relay.kafka.enabled` — required; defaults to `true` (set `false` only for unit tests)
- `KAFKA_BROKERS` / `relay.kafka.bootstrap-servers`
- `KAFKA_TOPIC` / `relay.kafka.topic`
- `KAFKA_CONSUMER_GROUP` / `relay.kafka.consumer.group-id`
- `KAFKA_TASK_TOPIC` / `relay.kafka.task-topic`
- `KAFKA_TASK_CONSUMER_GROUP` / `relay.kafka.task-consumer.group-id`
- `KAFKA_TASK_RETRY_TOPIC` / `relay.kafka.task-retry-topic`
- `KAFKA_RETRY_BACKOFF_ENABLED` / `relay.kafka.retry-backoff-enabled`

Host processes talk to Compose Kafka at `localhost:9092`. Containers use `kafka:9092`.
