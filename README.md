# Relay

A durable workflow orchestration engine built with Java, Spring Boot, PostgreSQL, and Kafka.

Relay executes dependency-aware task graphs while keeping workflow state durable in PostgreSQL. Tasks can be processed concurrently by distributed workers, retried after failures, and inspected through a REST API.

The project focuses on a few distributed-systems problems that appear in real workflow engines: durable state, dependency resolution, safe concurrent claiming, crash recovery, retries, and reliable event delivery.

## Architecture

```text
                    ┌─────────────────┐
                    │     Client      │
                    └────────┬────────┘
                             │ REST
                             ▼
                    ┌─────────────────┐
                    │   Relay API     │
                    │   Spring Boot   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │                 │
                    │ workflows       │
                    │ tasks           │
                    │ attempts        │
                    │ outbox          │
                    └───────┬─────────┘
                            │
                     transactional
                        outbox
                            │
                            ▼
                    ┌─────────────────┐
                    │      Kafka      │
                    │                 │
                    │ task events     │
                    │ retries         │
                    │ lifecycle events│
                    └────────┬────────┘
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
         ┌────────────────┐    ┌────────────────┐
         │ Relay Worker   │    │ Relay Worker   │
         │                │    │                │
         │ claim          │    │ claim          │
         │ execute        │    │ execute        │
         │ persist result │    │ persist result │
         └────────────────┘    └────────────────┘
```

PostgreSQL remains the source of truth for workflow and task state. Kafka is used as the distributed transport layer rather than as the authoritative workflow store.

## Features

- **Dependency-aware workflows** — tasks execute only after their dependencies complete.
- **Durable execution state** — workflows, tasks, and attempts are persisted in PostgreSQL.
- **Concurrent workers** — work can be processed across multiple worker instances.
- **Task claim leases** — abandoned work can be recovered after a worker disappears.
- **Retries and backoff** — failed tasks can be retried without losing workflow state.
- **Dead-letter handling** — exhausted work can be isolated instead of blocking execution.
- **Transactional outbox** — database state changes and events are coordinated without relying on unsafe database/broker dual writes.
- **Kafka transport** — workflow events and task execution can be distributed through Kafka.
- **Execution history** — task attempts provide an audit trail for failures and retries.
- **REST API** — submit workflows and inspect their current state.
- **Containerized local environment** — PostgreSQL, Kafka, API, and workers can run through Docker Compose.

## Reliability model

Relay treats PostgreSQL as the authoritative record of execution.

This is intentional. A workflow should not disappear because a worker crashes or a broker temporarily becomes unavailable.

### Durable task state

Task state is committed to PostgreSQL before another part of the workflow depends on it. Workers operate on persisted state rather than keeping workflow progress only in memory.

### Claim leases

Workers claim tasks for a limited period of time.

If a worker terminates while processing a task, its claim eventually expires and another worker can recover the work. This avoids requiring a single coordinator to permanently own a task.

### Idempotent execution

Because distributed delivery can happen more than once, task execution must tolerate redelivery.

Relay's execution model is designed around idempotent handling rather than assuming the message broker provides exactly-once delivery.

### Transactional outbox

Publishing an event directly after committing database state creates a dual-write problem:

```text
commit database state
        │
        ├── process crashes here
        │
        ▼
publish Kafka event
```

Relay avoids coupling those two independent writes directly.

Instead, the event is first recorded with the corresponding database transaction. An outbox publisher later delivers it to Kafka.

```text
┌──────────────────────────────┐
│ PostgreSQL transaction       │
│                              │
│ update workflow/task state   │
│ write outbox event           │
└──────────────┬───────────────┘
               │ commit
               ▼
        ┌──────────────┐
        │ Outbox worker│
        └──────┬───────┘
               │
               ▼
            Kafka
```

This makes retry and crash recovery explicit instead of depending on timing between PostgreSQL and Kafka.

## Tech stack

| Component | Technology |
|---|---|
| Language | Java 21 |
| Application framework | Spring Boot 3.4 |
| Persistence | PostgreSQL |
| ORM / data access | Spring Data JPA |
| Migrations | Flyway |
| Messaging | Apache Kafka |
| Build | Maven |
| Containers | Docker / Docker Compose |
| Testing | JUnit, Spring Boot Test, Testcontainers |

The repository is split into two Maven modules:

```text
Relay/
├── api/                 # REST API and application runtime
├── core/                # domain model and orchestration logic
├── .github/workflows/   # CI
├── docker-compose.yml
└── pom.xml
```

## Quick start

### Prerequisites

- Java 21
- Maven 3.9+
- Docker

Clone the repository:

```bash
git clone https://github.com/belalezat1/Relay.git
cd Relay
```

Create your environment file:

```bash
cp .env.example .env
```

### Run PostgreSQL only

```bash
docker compose up -d postgres
```

Then run the application:

```bash
mvn test

APP_ENV=dev \
DB_HOST=localhost \
DB_PORT=5432 \
DB_NAME=relay_dev \
DB_USERNAME=relay \
DB_PASSWORD=relay_dev \
APP_PORT=8080 \
mvn -pl api spring-boot:run
```

The API will be available at:

```text
http://localhost:8080
```

## Run the distributed stack

Relay also includes a Docker Compose profile for running PostgreSQL, Kafka, the API, and a separate worker.

```bash
docker compose --profile distributed up --build
```

The distributed profile separates API responsibilities from task consumption and enables Kafka-backed task processing.

## Submit a workflow

A workflow is submitted as a set of tasks and dependency relationships.

```bash
curl -X POST http://localhost:8080/api/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "tasks": [
      {
        "id": "fetch-data",
        "type": "success"
      },
      {
        "id": "process-data",
        "type": "success",
        "dependsOn": ["fetch-data"]
      },
      {
        "id": "publish-result",
        "type": "success",
        "dependsOn": ["process-data"]
      }
    ]
  }'
```

Relay stores the workflow and determines which tasks are eligible to execute based on their dependencies.

```text
fetch-data
    │
    ▼
process-data
    │
    ▼
publish-result
```

A DAG can also fan out:

```text
                 ┌──► validate ──┐
fetch-data ──────┤               ├──► publish
                 └──► transform ─┘
```

Only tasks whose dependencies have completed become eligible for execution.

## Configuration

Common runtime settings are exposed through environment variables.

### Database

```text
DB_HOST
DB_PORT
DB_NAME
DB_USERNAME
DB_PASSWORD
DB_POOL_SIZE
```

### Worker

```text
WORKER_MAX_CONCURRENCY
WORKER_POLL_DELAY
WORKER_BATCH_SIZE
WORKER_ORCHESTRATION_ENABLED
TASK_CLAIM_LEASE_SECONDS
WORKER_ID
```

### Kafka

```text
KAFKA_ENABLED
KAFKA_BROKERS
KAFKA_TOPIC
KAFKA_CONSUMER_GROUP

KAFKA_TASK_TOPIC
KAFKA_TASK_CONSUMER_GROUP
KAFKA_TASK_CONSUMER_ENABLED
KAFKA_TASK_CONCURRENCY
KAFKA_TASK_RETRY_TOPIC
KAFKA_TASK_RETRY_CONSUMER_GROUP
KAFKA_TOPIC_PARTITIONS
```

### Retry and outbox

```text
RETRY_BACKOFF_ENABLED
RETRY_MAX_ATTEMPTS

OUTBOX_POLL_DELAY
OUTBOX_BATCH_SIZE
OUTBOX_MAX_ATTEMPTS
```

See [`.env.example`](./.env.example) for the development defaults.

## Testing

Run the full Maven test suite:

```bash
mvn test
```

The project uses JUnit and Spring's testing stack, with Testcontainers available for integration testing against real infrastructure.

## Design principles

Relay intentionally keeps several responsibilities separate:

**PostgreSQL owns state.**  
Workflow correctness does not depend on Kafka retaining the only copy of execution state.

**Kafka transports work.**  
Messaging allows execution to move across process boundaries without turning the broker into the workflow database.

**Workers are replaceable.**  
A worker should be able to disappear without permanently owning the work it was executing.

**Delivery may repeat.**  
Retries and broker redelivery are expected, so correctness comes from durable state and idempotency rather than optimistic exactly-once assumptions.

**The API is not the worker.**  
The distributed runtime can separate request handling from task execution and scale them independently.

## License

MIT
